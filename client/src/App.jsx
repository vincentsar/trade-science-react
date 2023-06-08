import { useState, useEffect } from "react";
import ScrollCandleChart from "@/components/ScrollCandleChart";
import FlexBetween from "@/layouts/FlexBetween";
import Sidenav from "@/scenes/global/Sidenav";

// Date related
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";

const App = () => {
  const [assetData, setAssetData] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState(null);

  console.log(`Selected symbol: ${selectedSymbol}`);

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
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <div>
        <FlexBetween>
          <Sidenav data={assetData} onItemSelect={setSelectedSymbol} />
          <ScrollCandleChart />
          <ScrollCandleChart />
        </FlexBetween>
      </div>
    </LocalizationProvider>
  );
};

export default App;
