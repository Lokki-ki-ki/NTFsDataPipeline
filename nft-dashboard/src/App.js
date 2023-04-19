import "./App.css";
import React, { useState, useEffect } from "react";
import axios from "axios";
import Ranking from "./Components/Ranking";
import { Flex, Heading } from "@chakra-ui/react";

function App() {
  const [nftportAllTime, setNftportAllTime] = useState([]);

  useEffect(() => {
    // Make HTTP request to Flask backend
    axios
      .get("/nftport-all-time")
      .then((response) => {
        setNftportAllTime(response.data);
      })
      .catch((error) => {
        console.error(error);
      });
  }, []);

  return (
    <Flex alignItems={"center"} direction={"column"}>
      <Heading>NFT Dashboard</Heading>
      <Ranking data={nftportAllTime} />
    </Flex>
  );
}

export default App;
