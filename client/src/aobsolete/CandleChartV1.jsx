import { useState } from "react";
import {
  VictoryChart,
  VictoryCandlestick,
  VictoryArea,
  VictoryTheme,
  VictoryAxis,
  VictoryLine,
  VictoryScatter,
  VictoryTooltip,
  createContainer,
  VictoryLabel,
} from "victory";

// Generate some random candlestick data
const generateData = (count, yrange) => {
  var i = 0;
  var series = [];
  while (i < count) {
    series.push({
      x: new Date(2023, 1, i),
      open:
        Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min,
      close:
        Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min,
      high:
        Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min,
      low:
        Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min,
    });
    i++;
  }
  return series;
};

const chartData = generateData(90, { min: 10, max: 90 });
const VictoryVoronoiCursorContainer = createContainer("voronoi", "cursor");
//Note cursor doesn't work for now... createContainer only combines 2 containers

function App() {
  const width = 600;
  const height = 350;

  // Calculate the mid-point of the chart data
  const midPoint = Math.floor(chartData.length / 2);

  // Prepare the area data
  const areaData = [
    { x: chartData[0].x, y: 100 },
    { x: chartData[midPoint].x, y: 100 },
  ];

  // Calculate profit/loss
  const profit = chartData[midPoint].close > chartData[0].open;

  return (
    <div className="chart-container">
      <VictoryChart
        theme={VictoryTheme.material}
        domainPadding={{ x: 5 }}
        padding={{ top: 40, bottom: 40, left: 40, right: 40 }}
        scale={{ x: "time" }}
        width={width}
        height={height}
        containerComponent={
          <VictoryVoronoiCursorContainer
            cursorLabel={({ datum }) => `x: ${datum.x}, y: ${datum.y}`}
            voronoiDimension="x"
            labels={({ datum }) => {
              if (datum.open && datum.high && datum.low && datum.close) {
                return `D: ${datum.x} O: ${datum.open} H: ${datum.high} L: ${datum.low} C: ${datum.close}`;
              } else {
                return;
              }
            }}
            labelComponent={
              <VictoryTooltip
                center={{ x: 70, y: 20 }}
                cornerRadius={0}
                flyoutStyle={{ stroke: "black", strokeWidth: 0 }}
              />
            }
          />
        }
        style={{
          background: { fill: "white" },
          parent: {
            border: "1px solid #ccc",
            background: "rgba(0,0,0,0.05)",
            cursor: "crosshair",
            padding: "0px",
          },
        }}
      >
        <VictoryAxis
          dependentAxis
          style={{
            grid: { stroke: "#e4e4e4" },
            ticks: { stroke: "black" },
            tickLabels: { fill: "black" },
          }}
        />

        <VictoryAxis
          tickFormat={(x) => new Date(x).toLocaleDateString()}
          style={{
            grid: { stroke: "#e4e4e4" },
            ticks: { stroke: "black" },
            tickLabels: { fill: "black" },
          }}
        />
        <VictoryArea
          style={{
            data: {
              fill: profit ? "rgba(0, 255, 0, 0.25)" : "rgba(255, 0, 0, 0.25)",
            },
          }}
          data={areaData}
        />
        <VictoryLine
          style={{ data: { stroke: profit ? "green" : "red" } }}
          data={[
            { x: chartData[0].x, y: chartData[0].open },
            { x: chartData[midPoint].x, y: chartData[midPoint].close },
          ]}
        />
        <VictoryScatter
          style={{ data: { fill: profit ? "green" : "red" } }}
          data={[{ x: chartData[0].x, y: chartData[0].open }]}
        />
        <VictoryScatter
          style={{ data: { fill: profit ? "green" : "red" } }}
          data={[{ x: chartData[midPoint].x, y: chartData[midPoint].close }]}
        />
        <VictoryCandlestick
          candleColors={{ positive: "#00FF00", negative: "#c43a31" }}
          data={chartData}
          style={{
            data: {
              stroke: ({ datum }) =>
                datum.close > datum.open ? "#00FF00" : "#c43a31",
            },
          }}
        />
      </VictoryChart>
    </div>
  );
}

export default App;
