import React, { useState } from "react";
import {
  Box,
  Flex,
  Link,
  Button,
  HStack,
  useColorModeValue,
  Stack,
} from "@chakra-ui/react";
import { MoonIcon, SunIcon } from "@chakra-ui/icons";

const Links = ["Top Selling NFTs", "NFT Transactions", "Streaming"];
const NAV_ITEMS = [
  {
    label: "Top Selling NFTs",
    href: "topSellingNFTs",
  },
  {
    label: "NFT Transactions",
    href: "#",
  },
  {
    label: "Streaming",
    href: "#",
  },
];

const NavLink = ({ children }) => (
  <Link
    px={2}
    py={1}
    rounded={"md"}
    _hover={{
      textDecoration: "none",
      bg: useColorModeValue("gray.200", "gray.700"),
    }}
    href={children.href}
  >
    {children.label}
  </Link>
);

export default function Nav() {
  const [colorMode, setColorMode] = useState("light");

  const toggleColorMode = () => {
    setColorMode(colorMode === "light" ? "dark" : "light");
  };

  return (
    <>
      <Box bg={colorMode === "light" ? "gray.100" : "gray.900"} px={4}>
        <Flex h={16} alignItems={"center"} justifyContent={"space-between"}>
          <Box>NFT Dashboard</Box>
          <HStack as={"nav"} spacing={4} display={{ base: "none", md: "flex" }}>
            {NAV_ITEMS.map((link) => (
              <NavLink key={link}>{link}</NavLink>
            ))}
          </HStack>
          <Flex alignItems={"center"}>
            <Stack direction={"row"} spacing={7}>
              <Button onClick={toggleColorMode}>
                {colorMode === "light" ? <MoonIcon /> : <SunIcon />}
              </Button>
            </Stack>
          </Flex>
        </Flex>
      </Box>
    </>
  );
}
