import path from 'path'
import {In} from 'typeorm'
import {HttpClient} from '@subsquid/http-client'
import {TypeormDatabase} from '@subsquid/typeorm-store'
import * as erc721 from './abi/erc721'
import {Multicall} from './abi/multicall'
import {Attribute, Owner, Token, Transfer} from './model'
import {Block, CONTRACT_ADDRESS, Context, Log, MULTICALL_ADDRESS, Transaction, processor} from './processor'
import {assertNotNull} from '@subsquid/evm-processor'

export const MUTLTICALL_BATCH_SIZE = 50

// maximal number of requests to the IPFS gateway per second
// Use a private gateway to increase the number and speed up
// the indexing
export const MAX_IPFS_REQ_SEC = 25
// replace with a private gateway to avoid rate limits and allow bigger MAX_IPFS_REQ_SEC
export const IPFS_GATEWAY = 'https://ipfs.io'

processor.run(new TypeormDatabase({supportHotBlocks: true}), async (ctx) => {
    let transfersData: TransferEvent[] = []

    for (let block of ctx.blocks) {
        for (let log of block.logs) {
            if (log.topics[0] === erc721.events.Transfer.topic) {
                transfersData.push(getTransfer(ctx, log))
            }
        }
    }

    await processTransfers(ctx, transfersData)
})

interface TransferEvent {
    id: string
    block: Block
    transaction: Transaction
    from: string
    to: string
    tokenIndex: bigint
}

function getTransfer(ctx: Context, log: Log): TransferEvent {
    let transaction = assertNotNull(log.transaction, 'Missing transaction')

    let event = erc721.events.Transfer.decode(log)

    let from = event.from.toLowerCase()
    let to = event.to.toLowerCase()
    let tokenIndex = event.tokenId

    ctx.log.debug({block: log.block, txHash: transaction.hash}, `Transfer from ${from} to ${to} token ${tokenIndex}`)
    return {
        id: log.id,
        block: log.block,
        transaction,
        tokenIndex,
        from,
        to,
    }
}

async function processTransfers(ctx: Context, transfersData: TransferEvent[]) {
    let tokensIds: Set<string> = new Set()
    let ownersIds: Set<string> = new Set()

    for (let transferData of transfersData) {
        tokensIds.add(transferData.tokenIndex.toString())
        ownersIds.add(transferData.from)
        ownersIds.add(transferData.to)
    }

    let transfers: Transfer[] = []

    let tokens = await ctx.store.findBy(Token, {id: In([...tokensIds])}).then((q) => new Map(q.map((i) => [i.id, i])))
    let owners = await ctx.store.findBy(Owner, {id: In([...ownersIds])}).then((q) => new Map(q.map((i) => [i.id, i])))

    let newTokens: Token[] = []
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
                index: transferData.tokenIndex,
            })
            tokens.set(token.id, token)
            newTokens.push(token)
        }
        token.owner = to

        let {id, block, transaction} = transferData

        let transfer = new Transfer({
            id,
            blockNumber: block.height,
            timestamp: new Date(block.timestamp),
            txHash: transaction.hash,
            from,
            to,
            token,
        })

        transfers.push(transfer)
    }

    await initTokens(ctx, newTokens)

    await ctx.store.upsert([...owners.values()])
    await ctx.store.upsert([...tokens.values()])
    await ctx.store.upsert(transfers)
}

async function initTokens(ctx: Context, tokens: Token[]) {
    let block = ctx.blocks[ctx.blocks.length - 1].header
    let multicall = new Multicall(ctx, block, MULTICALL_ADDRESS)

    let tokenURIs = await multicall.aggregate(
        erc721.functions.tokenURI,
        CONTRACT_ADDRESS,
        tokens.map((t) => [t.index]),
        MUTLTICALL_BATCH_SIZE // to prevent timeout we will use paggination
    )

    let metadatas: (TokenMetadata | undefined)[] = []
    for (let batch of splitIntoBatches(tokenURIs, MAX_IPFS_REQ_SEC)) {
        let m = await Promise.all(
            batch.map(async (uri, index) => {
                // spread out the requests evenly within a second interval
                return sleep(Math.ceil((1000 * (index + 1)) / MAX_IPFS_REQ_SEC)).then(() =>
                    fetchTokenMetadata(ctx, uri)
                )
            })
        )
        metadatas.push(...m)
    }

    for (let i = 0; i < tokens.length; i++) {
        tokens[i].uri = tokenURIs[i]
        tokens[i].image = metadatas[i]?.image
        tokens[i].attributes = metadatas[i]?.attributes.map(
            (a) =>
                new Attribute({
                    traitType: a.trait_type,
                    value: a.value,
                })
        )
    }
}

const ipfsRegExp = /^ipfs:\/\/(.+)$/
const client = new HttpClient({
    headers: {'Content-Type': 'application/json'},
})

interface TokenMetadata {
    image: string
    attributes: {
        trait_type: string
        value: string
    }[]
}

async function fetchTokenMetadata(ctx: Context, uri: string): Promise<TokenMetadata | undefined> {
    try {
        if (uri.startsWith('ipfs://')) {
            const gatewayURL = path.posix.join(IPFS_GATEWAY, ipfsRegExp.exec(uri)![1])
            let res = await client.get(gatewayURL)
            ctx.log.info(`Successfully fetched metadata from ${gatewayURL}`)
            return res
        } else if (uri.startsWith('http://') || uri.startsWith('https://')) {
            let res = await client.get(uri)
            ctx.log.info(`Successfully fetched metadata from ${uri}`)
            return res
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
function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
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
