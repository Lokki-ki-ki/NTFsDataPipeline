import React from 'react';
import { Box, Heading, Text, Card, CardHeader, Stack, StackDivider, CardBody, Image } from '@chakra-ui/react';
import PriceVSMarketplace from '../Assets/pvm.png';
import LineChart from '../Components/LineChart';

function Visualizations() {
    return(
    <Card>
        <CardHeader>
            <Heading size='md'>Visualizations</Heading>
            <Text pt='2' fontSize='sm'>
                        Made using matplotlib
                    </Text>
        </CardHeader>

        <CardBody>
            <Stack divider={<StackDivider />} spacing='4'>
                <Box>
                    <Heading size='xs' textTransform='uppercase'>
                        Summary of Price VS Marketplace
                    </Heading>
                    <Image src={PriceVSMarketplace} />

             
                </Box>
                <Box>
                    <Heading size='xs' textTransform='uppercase'>
                        Overview
                    </Heading>
                    <LineChart></LineChart>
                </Box>
                <Box>
                    <Heading size='xs' textTransform='uppercase'>
                        Analysis
                    </Heading>
                    <Text pt='2' fontSize='sm'>
                        See a detailed analysis of all your business clients.
                    </Text>
                </Box>
            </Stack>
        </CardBody>
    </Card>
    );
}

export default Visualizations;