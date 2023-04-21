import "./App.css";
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import TopSellingNFTs from "./Pages/TopSellingNFTs";
import Visualizations from "./Pages/Visualizations";


function App() {
  return (
    <>
      <Router basename={"/NTFsDataPipeline"}>
          <Routes>
            <Route path="/topSellingNFTs" element={<TopSellingNFTs />} />
            <Route exact path="/" element={<TopSellingNFTs />} />
            <Route path="/visualizations" element={<Visualizations />} />
          </Routes>
      
      </Router>
    </>
  );
}

export default App;
