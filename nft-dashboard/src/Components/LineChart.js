import React from "react";
import ReactApexChart from "react-apexcharts";
import axios from "axios";

const apiClient = axios.create({baseURL: 'https://proxy.cors.sh/https://backend-pyvsd2ddxq-as.a.run.app'})
class LineChart extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      chartData: [],
      chartOptions: {},
    };
  }

  componentDidMount() {
    fetch("/nft_data") // Change this to match your backend route
      .then((response) => response.json())
      .then((data) => {
        this.setState({
          chartData: data,
          chartOptions: {}, // Add your chart options here
        });
      });
  }

  render() {
    return (
      <ReactApexChart
        options={this.state.chartOptions}
        series={this.state.chartData}
        type='area'
        width='100%'
        height='100%'
      />
    );
  }
}

export default LineChart;
