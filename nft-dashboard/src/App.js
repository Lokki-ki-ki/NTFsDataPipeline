import "./App.css";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import TopSellingNFTs from "./Pages/TopSellingNFTs";


function App() {
  return (
    <>
      <Router basename={"/NTFsDataPipeline"}>
        <div className="container">
          <TopSellingNFTs />
          <Routes>
            {/*<Route path="/topSellingNFTs" component={<TopSellingNFTs />} />*/}
            <Route exact path="/" component={<TopSellingNFTs />} />
          </Routes>
        </div>
        <div className="footer"></div>
      </Router>
    </>
  );
}

export default App;
