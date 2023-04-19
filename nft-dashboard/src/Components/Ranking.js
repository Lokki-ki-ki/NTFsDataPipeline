import React from "react";
import {
  Table,
  Thead,
  Tbody,
  Wrap,
  Tr,
  Th,
  Td,
  TableCaption,
  TableContainer,
  Avatar,
  WrapItem,
} from "@chakra-ui/react";

function Ranking({ data }) {
  return (
    <TableContainer>
      <Table variant="striped" style={{ tableLayout: "fixed", width: "100%" }}>
        <TableCaption>Top Selling NFTs</TableCaption>
        <Thead>
          <Tr>
            <Th>Rank</Th>
            <Th>NFT</Th>
            <Th>Chain</Th>
            <Th>Description</Th>
            <Th>Address</Th>
          </Tr>
        </Thead>
        <Tbody>
          {data.map((nft) => (
            <Tr key={nft.rank}>
              <Td>{nft.rank}</Td>
              <Td>
                <Wrap>
                  <WrapItem>
                    <Avatar src={nft.picture} />
                  </WrapItem>
                  <WrapItem>{nft.name}</WrapItem>
                </Wrap>
              </Td>
              <Td>{nft.chain}</Td>
              <Td style={{ whiteSpace: "pre-wrap" }}>{nft.description}</Td>
              <Td style={{ whiteSpace: "pre-wrap" }}>{nft.contract_address}</Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </TableContainer>
  );
}

export default Ranking;
