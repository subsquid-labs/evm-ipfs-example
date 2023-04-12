import assert from 'assert'
import https from 'https'
import path from 'path'
import axios from 'axios'
import {BigNumber} from 'ethers'
import {In} from 'typeorm'
import {lookupArchive} from '@subsquid/archive-registry'
import {BatchHandlerContext, BatchProcessorItem, EvmBatchProcessor, EvmBlock} from '@subsquid/evm-processor'
import {LogItem} from '@subsquid/evm-processor/lib/interfaces/dataSelection'
import {Store, TypeormDatabase} from '@subsquid/typeorm-store'
import * as erc721 from './abi/erc721'
import {Multicall} from './abi/multicall'
import {Attribute, Owner, Token, Transfer} from './model'

export const CONTRACT_ADDRESS = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'
export const MULTICALL_ADDRESS = '0x5ba1e12693dc8f9c48aad8770482f4739beed696'
export const MUTLTICALL_BATCH_SIZE = 1000

// maximal number of requests to the IPFS gateway per second
// Use a private gateway to increase the number and speed up
// the indexing
export const MAX_IPFS_REQ_SEC = 1
// replace with a private gateway to avoid rate limits and allow bigger MAX_IPFS_REQ_SEC
export const IPFS_GATEWAY = 'https://subsquid.myfilebase.com/ipfs/'

let database = new TypeormDatabase()
let processor = new EvmBatchProcessor()
    .setDataSource({
        archive: lookupArchive('eth-mainnet'),
        // replace with a private endpoint for better performance
        chain: 'https://rpc.ankr.com/eth',
    })
    .setBlockRange({
        from: 12_287_507,
    })
    .addLog(CONTRACT_ADDRESS, {
        filter: [[erc721.events.Transfer.topic]],
        data: {
            evmLog: {
                topics: true,
                data: true,
            },
            transaction: {
                hash: true,
            },
        },
    })

type Item = BatchProcessorItem<typeof processor>
type Context = BatchHandlerContext<Store, Item>

processor.run(database, async (ctx) => {
    let transfersData: TransferEventData[] = []

    for (let block of ctx.blocks) {
        for (let item of block.items) {
            if (item.kind !== 'evmLog') continue

            if (item.evmLog.topics[0] === erc721.events.Transfer.topic) {
                transfersData.push(handleTransfer(ctx, block.header, item))
            }
        }
    }

    await saveTransfers(ctx, transfersData)
})

interface TransferEventData {
    id: string
    blockNumber: number
    timestamp: Date
    txHash: string
    from: string
    to: string
    tokenIndex: bigint
}

function handleTransfer(
    ctx: Context,
    block: EvmBlock,
    item: LogItem<{evmLog: {topics: true; data: true}; transaction: {hash: true}}>
): TransferEventData {
    let {from, to, tokenId} = erc721.events.Transfer.decode(item.evmLog)

    let transfer: TransferEventData = {
        id: item.evmLog.id,
        tokenIndex: tokenId.toBigInt(),
        from,
        to,
        timestamp: new Date(block.timestamp),
        blockNumber: block.height,
        txHash: item.transaction.hash,
    }

    return transfer
}

async function saveTransfers(ctx: Context, transfersData: TransferEventData[]) {
    let tokensIds: Set<string> = new Set()
    let ownersIds: Set<string> = new Set()

    for (let transferData of transfersData) {
        tokensIds.add(transferData.tokenIndex.toString())
        ownersIds.add(transferData.from)
        ownersIds.add(transferData.to)
    }

    let tokens = await ctx.store.findBy(Token, {id: In([...tokensIds])}).then((q) => new Map(q.map((i) => [i.id, i])))
    let owners = await ctx.store.findBy(Owner, {id: In([...ownersIds])}).then((q) => new Map(q.map((i) => [i.id, i])))

    let transfers: Transfer[] = []
    for (let transferData of transfersData) {
        let from = owners.get(transferData.from)
        if (from == null) {
            from = new Owner({id: transferData.from})
            owners.set(from.id, from)
        }

        let to = owners.get(transferData.to)
        if (to == null) {
            to = new Owner({id: transferData.to})
            owners.set(to.id, to)
        }

        let tokenId = transferData.tokenIndex.toString()
        let token = tokens.get(tokenId)
        if (token == null) {
            token = new Token({
                id: tokenId,
                index: BigInt(tokenId),
            })
            tokens.set(token.id, token)
        }
        token.owner = to

        let {id, blockNumber, txHash, timestamp} = transferData

        let transfer = new Transfer({
            id,
            blockNumber,
            timestamp,
            txHash,
            from,
            to,
            token,
        })

        transfers.push(transfer)
    }

    await fetchTokens(ctx, last(ctx.blocks).header, [...tokens.values()])

    await ctx.store.save([...owners.values()])
    await ctx.store.save([...tokens.values()])

    await ctx.store.insert(transfers)
}

async function fetchTokens(ctx: Context, block: EvmBlock, tokens: Token[]) {
    let contract = new Multicall(ctx, block, MULTICALL_ADDRESS)

    let tokenURIs = await contract.aggregate(
        erc721.functions.tokenURI,
        CONTRACT_ADDRESS,
        tokens.map((t) => [BigNumber.from(t.index)]),
        MUTLTICALL_BATCH_SIZE // to prevent timeout we will use paggination
    )

    let metadatas: (TokenMetadata | undefined)[] = []
    for (let batch of splitIntoBatches(tokenURIs, MAX_IPFS_REQ_SEC)) {
        let m = await Promise.all(batch.map((uri, index) => {
            // spread out the requests evenly within a second interval
            return sleep(Math.ceil(1000*(index+1)/MAX_IPFS_REQ_SEC)).then(() => fetchTokenMetadata(ctx, uri))
        }))
        metadatas.push(...m)
    }

    for (let i = 0; i < tokens.length; i++) {
        tokens[i].uri = tokenURIs[i]
        tokens[i].image = metadatas[i]?.image
        tokens[i].attributes = metadatas[i]?.attributes
    }
}

interface TokenMetadata {
    image: string
    attributes: Attribute[]
}

const client = axios.create({
    headers: {'Content-Type': 'application/json'},
    httpsAgent: new https.Agent({keepAlive: true}),
    transformResponse(res: string): TokenMetadata {
        let data: {image: string; attributes: {trait_type: string; value: string}[]} = JSON.parse(res)
        return {
            image: data.image,
            attributes: data.attributes.map((a) => new Attribute({traitType: a.trait_type, value: a.value})),
        }
    },
})

const ipfsRegExp = /^ipfs:\/\/(.+)$/

async function fetchTokenMetadata(ctx: Context, uri: string): Promise<TokenMetadata | undefined> {
    try {
        if (uri.startsWith('ipfs://')) {
            const gatewayURL = path.posix.join(IPFS_GATEWAY, ipfsRegExp.exec(uri)![1])
            let res = await client.get(gatewayURL)
            ctx.log.info(`Successfully fetched metadata from ${gatewayURL}`)
            return res.data
        } else if (uri.startsWith('http://') || uri.startsWith('https://')) {
            let res = await client.get(uri)
            ctx.log.info(`Successfully fetched metadata from ${uri}`)
            return res.data
        } else {
            ctx.log.warn(`Unexpected metadata URL protocol: ${uri}`)
            return undefined
        }
    } catch (e) {
        throw new Error(`failed to fetch metadata at ${uri}. Error: ${e}`)
    }
}

//////////////////////////////
// Utility functions 
/////////////////////////////
function last<T>(arr: T[]): T {
    assert(arr.length > 0)
    return arr[arr.length - 1]
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function* splitIntoBatches<T>(list: T[], maxBatchSize: number): Generator<T[]> {
    if (list.length <= maxBatchSize) {
        yield list
    } else {
        let offset = 0
        while (list.length - offset > maxBatchSize) {
            yield list.slice(offset, offset + maxBatchSize)
            offset += maxBatchSize
        }
        yield list.slice(offset)
    }
}
