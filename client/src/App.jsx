import { useState, useEffect } from "react";
import ScrollCandleChart from "@/components/ScrollCandleChart";
import FlexBetween from "@/layouts/FlexBetween";
// import MemberList from "./components/MemberList";

const App = () => {
  const [data, setData] = useState([]);

  // useEffect(() => {
  //   fetch("members")
  //     .then((res) => res.json())
  //     .then((data) => {
  //       setData(data);
  //       console.log(data);
  //     })
  //     .catch((error) => {
  //       console.error("Error fetching data:", error);
  //     });
  // }, []);

  return (
    <div>
      {/* <MemberList data={data} /> */}
      <FlexBetween>
        <ScrollCandleChart />
        <ScrollCandleChart />
      </FlexBetween>
    </div>
  );
};

export default App;
