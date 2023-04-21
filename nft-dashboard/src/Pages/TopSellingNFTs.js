import React, { useState, useEffect } from "react";
import axios from "axios";
import Ranking from "../Components/Ranking";
import { Flex, Heading } from "@chakra-ui/react";
import { Menu, MenuButton, MenuList, MenuItem, Button } from "@chakra-ui/react";


const apiClient = axios.create({baseURL: 'http://backend-pyvsd2ddxq-as.a.run.app'})

function TopSellingNFTs() {
  const [nftportAllTime, setNftportAllTime] = useState([]);
  const [selectedList, setSelectedList] = useState("Daily");

  const handleListClick = (item) => {
    setSelectedList(item);
    setNftportAllTime([]);
  };

  useEffect(() => {
    if (selectedList === "All Time") {
        apiClient
        .get("/nftport-all-time")
        .then((response) => {
          setNftportAllTime(response.data);
        })
        .catch((error) => {
          console.error(error);
        });
    } else if (selectedList === "Daily") {
          apiClient.get('/nftport-all-time', {
            headers: {
              'X-XSRF-TOKEN': 'abc123'
            }
          })
        .then((response) => {
          console.log(response.data)
          setNftportAllTime(response.data);
        })
        .catch((error) => {
          console.error(error.err);
        });
    } else if (selectedList === "Daily") {
      apiClient
        .get("/nftport-daily")
        .then((response) => {
          setNftportAllTime(response.data);
        })
        .catch((error) => {
          console.error(error);
        });
    } else if (selectedList === "Weekly") {
        apiClient
        .get("/nftport-weekly")
        .then((response) => {
          setNftportAllTime(response.data);
        })
        .catch((error) => {
          console.error(error);
        });
    } else if (selectedList === "Monthly") {
        apiClient
        .get("/nftport-monthly")
        .then((response) => {
          setNftportAllTime(response.data);
        })
        .catch((error) => {
          console.error(error);
        });
    }
  }, [selectedList]);

  return (
    <Flex alignItems={"center"} direction={"column"}>
      <Heading mt={4} mb={4}>
        Top Selling NFTs
      </Heading>
      <Menu>
        <MenuButton as={Button} w="224px" mt={4} mb={4}>
          {selectedList ? selectedList : "Menu"}
        </MenuButton>
        <MenuList w="100%">
          <MenuItem onClick={() => handleListClick("Daily")}>Daily</MenuItem>
          <MenuItem onClick={() => handleListClick("Weekly")}>Weekly</MenuItem>
          <MenuItem onClick={() => handleListClick("Monthly")}>
            Monthly
          </MenuItem>
          <MenuItem onClick={() => handleListClick("All Time")}>
            All Time
          </MenuItem>
        </MenuList>
      </Menu>
      <Ranking data={nftportAllTime} />
    </Flex>
  );
}

export default TopSellingNFTs;
