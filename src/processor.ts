import assert from 'assert'
import https from 'https'
import path from 'path'
import {In} from 'typeorm'
import {lookupArchive} from '@subsquid/archive-registry'
import {
    BlockHeader,
    DataHandlerContext,
    EvmBatchProcessor,
    EvmBatchProcessorFields,
    Log as _Log,
    Transaction as _Transaction,
} from '@subsquid/evm-processor'
import {Store, TypeormDatabase} from '@subsquid/typeorm-store'
import * as erc721 from './abi/erc721'
import {Multicall} from './abi/multicall'
import {Attribute, Owner, Token, Transfer} from './model'

export const CONTRACT_ADDRESS = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'
export const MULTICALL_ADDRESS = '0xeefba1e63905ef1d7acba5a8513c70307c1ce441'

export const processor = new EvmBatchProcessor()
    .setDataSource({
        archive: lookupArchive('eth-mainnet'),
        // replace with a private endpoint for better performance
        chain: 'https://rpc.ankr.com/eth',
    })
    .setFinalityConfirmation(10)
    .setBlockRange({
        from: 12_287_507,
    })
    .setFields({
        evmLog: {
            topics: true,
            data: true,
        },
        transaction: {
            hash: true,
        },
    })
    .addLog({
        address: [CONTRACT_ADDRESS],
        topic0: [erc721.events.Transfer.topic],
        transaction: true,
    })

export type Fields = EvmBatchProcessorFields<typeof processor>
export type Context = DataHandlerContext<Store, Fields>
export type Block = BlockHeader<Fields>
export type Log = _Log<Fields>
export type Transaction = _Transaction<Fields>
