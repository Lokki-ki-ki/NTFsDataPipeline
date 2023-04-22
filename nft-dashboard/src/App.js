import "./App.css";
import { Routes, Route, BrowserRouter as Router } from 'react-router-dom';
import TopSellingNFTs from "./Pages/TopSellingNFTs";
import Visualizations from "./Pages/Visualizations";


function App() {
  return (
    <>
      <div className="container">
        <Router basename="/NTFsDataPipeline">
          <Routes>
            <Route exact path="/" element={<TopSellingNFTs />} />
            <Route path="/topSellingNFTs" element={<TopSellingNFTs />} />
            <Route path="/visualizations" element={<Visualizations />} />
          </Routes>
        </Router>
      </div>
    </>
  );
}

export default App;
