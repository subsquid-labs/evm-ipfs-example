
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/subsquid-labs/ipfs-example)

The squid indexes transfers and metadata of the Bored Apes Yacht Club (BAYC) NFTs by watching `Transfer` event logs. The NFT token metadata is fetching from the URLs stored in the token contract, and it is optimized for batch processing:
- Contract calls are batched using the [Multicall contract](https://docs.subsquid.io/evm-indexing/squid-evm-typegen/#batching-contract-state-calls-using-the-multicall-contract)
- IPFS gateway calls are batched and evenly spread out to avoid rate-limiting. Use private gateways and the `MAX_IPFS_REQ_SEC` to increase the indexing speed.

One can use this example as a template for scaffolding a new squid project with [`sqd init`](https://docs.subsquid.io/squid-cli/):

```bash
sqd init my-new-squid --template https://github.com/subsquid-labs/ipfs-example
```


## Prerequisites

- Node v16.x
- Docker
- [Squid CLI](https://docs.subsquid.io/squid-cli/)

## Running 

Clone the repo and navigate to the root folder.

```bash
npm ci
sqd build
# start the database
sqd up
# starts a long-running ETL and blocks the terminal
sqd process

# starts the GraphQL API server at localhost:4350/graphql
sqd serve
```
