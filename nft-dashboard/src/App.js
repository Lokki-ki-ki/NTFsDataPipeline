import "./App.css";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import TopSellingNFTs from "./Pages/TopSellingNFTs";

function App() {
  return (
    <>
      <Router>
        <div className="container">
          <Routes>
            <Route path="/" element={<TopSellingNFTs />} />
            <Route path="/topSellingNFTs" element={<TopSellingNFTs />} />
          </Routes>
        </div>
        <div className="footer"></div>
      </Router>
    </>
  );
}

export default App;
