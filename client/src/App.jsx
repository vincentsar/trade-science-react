import { useState, useEffect } from "react";
import ScrollCandleChart from "@/components/ScrollCandleChart";
import FlexBetween from "@/layouts/FlexBetween";
import Sidenav from "@/scenes/global/Sidenav";

const App = () => {
  const [assetData, setAssetData] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState(null);

  console.log("selectedSymbol", selectedSymbol);

  useEffect(() => {
    fetch("http://127.0.0.1:6050/assets")
      .then((res) => res.json())
      .then((data) => {
        setAssetData(data);
      })
      .catch((error) => {
        console.error("Error fetching data:", error);
      });
  }, []);

  return (
    <div>
      <FlexBetween>
        <Sidenav data={assetData} onItemSelect={setSelectedSymbol} />
        <ScrollCandleChart />
        <ScrollCandleChart />
      </FlexBetween>
    </div>
  );
};

export default App;
