import { useState, useEffect, useRef } from "react";
import { Box } from "@mui/material";
import {
  VictoryCandlestick,
  VictoryChart,
  VictoryAxis,
  VictoryBar,
  VictoryLabel,
  VictoryLine,
  VictoryTooltip,
  createContainer,
} from "victory";

import {
  processDatumToStr,
  processChartDataChange,
} from "@/data/processChartData";
import { sampleData } from "@/data/sampleData";

// Data format
let chartData = processChartDataChange(sampleData);
const latestClose = chartData[chartData.length - 1].close;

// Visual chart variables
const minDataPoints = 6;
const maxDataPoints = 50;

const pxDomainXPadding = 15;
const pxDomainYPadding = 5;

const pxChartPadding = 40;
const pxChartBetweenPadding = 5;
const remChartFontSize = "5rem";

const pxChartCandleHeight = 600;
const pxChartVolumeHeight = 200;

const pxChartCandleWidth = 1000;
const pxChartVolumeWidth = 1000;

const clrGreen = "#00ff00";
const clrRed = "#c43a31";
const clrBGLightPixelLine = {
  grid: { stroke: "#e4e4e4", strokeWidth: 1, strokeDasharray: "2,2" },
};

// Functional chart variables
const VictoryVoronoiCursorContainer = createContainer("cursor", "voronoi");

const App = () => {
  const [dataIndex, setDataIndex] = useState(0);
  const [dataToShow, setDataToShow] = useState(chartData);
  const chartContainer = useRef(); //Stop default scroll event's propagation
  const [maxHigh, setMaxHigh] = useState(
    Math.max(...chartData.map((d) => d.volume))
  );

  useEffect(() => {
    setDataToShow(chartData.slice(dataIndex, dataIndex + maxDataPoints));
  }, [dataIndex]);

  useEffect(() => {
    setMaxHigh(Math.max(...dataToShow.map((d) => d.high)));
  }, [dataToShow]);

  useEffect(() => {
    const handleWheel = (event) => {
      event.preventDefault();
      setDataIndex((oldIndex) => {
        let newIndex = oldIndex - Math.sign(event.deltaY);
        if (newIndex <= 0) {
          newIndex = 0;
        } else if (newIndex > chartData.length - minDataPoints) {
          newIndex = chartData.length - minDataPoints;
        }
        return newIndex;
      });
    };

    const chartDiv = chartContainer.current;
    chartDiv.addEventListener("wheel", handleWheel, { passive: false });

    return () => {
      chartDiv.removeEventListener("wheel", handleWheel);
    };
  }, []);

  return (
    <div ref={chartContainer}>
      <Box>
        <VictoryChart
          domainPadding={{ x: pxDomainXPadding, y: pxDomainYPadding }}
          width={pxChartCandleWidth}
          height={pxChartCandleHeight}
          style={{ parent: { border: "1px solid #e4e4e4" } }}
          padding={{
            top: pxChartPadding,
            left: pxChartPadding,
            right: pxChartPadding,
            bottom: -pxChartBetweenPadding,
          }}
          containerComponent={
            <VictoryVoronoiCursorContainer
              cursorLabelComponent={<VictoryLabel style={{ fontSize: 0 }} />}
              cursorLabel={(datum) => {
                return `$${datum.y}`;
              }}
              voronoiDimension="x"
              labels={({ datum }) => {
                if (
                  datum.x &&
                  datum.open &&
                  datum.close &&
                  datum.low &&
                  datum.high
                ) {
                  return processDatumToStr(datum);
                } else {
                  return;
                }
              }}
              labelComponent={
                // Flyout style is needed to remove style for first item
                <VictoryTooltip
                  style={{ fontSize: remChartFontSize, textAnchor: "start" }}
                  center={{ x: 100, y: 20 }}
                  cornerRadius={0}
                  flyoutStyle={{
                    stroke: "black",
                    strokeWidth: 0,
                    fill: "none",
                  }}
                />
              }
            />
          }
        >
          <VictoryLine
            style={{
              data: {
                stroke: clrGreen,
                strokeWidth: 1,
                strokeDasharray: "5,5",
              },
            }}
            data={[
              { x: dataToShow[0].x, y: latestClose },
              { x: dataToShow[dataToShow.length - 1].x, y: latestClose },
            ]}
          />
          <VictoryAxis
            dependentAxis
            tickLabelComponent={
              <VictoryLabel style={{ fontSize: remChartFontSize }} />
            }
            style={clrBGLightPixelLine}
          />
          <VictoryAxis style={clrBGLightPixelLine}></VictoryAxis>
          <VictoryCandlestick
            candleColors={{ positive: clrGreen, negative: clrRed }}
            data={dataToShow}
            close="close"
            open="open"
            high="high"
            low="low"
            style={{
              data: {
                stroke: ({ datum }) =>
                  datum.close > datum.open ? "#00FF00" : "#c43a31",
              },
            }}
          />
        </VictoryChart>
      </Box>
      <Box>
        <VictoryChart
          domainPadding={{ x: pxDomainXPadding, y: pxDomainYPadding }}
          width={pxChartVolumeWidth}
          height={pxChartVolumeHeight}
          style={{ parent: { border: "1px solid #e4e4e4" } }}
          padding={{
            top: pxChartBetweenPadding,
            bottom: pxChartPadding,
            left: pxChartPadding,
            right: pxChartPadding,
          }}
        >
          <VictoryAxis
            tickFormat={(x) => new Date(x).toLocaleDateString()}
            tickLabelComponent={
              <VictoryLabel style={{ fontSize: remChartFontSize }} />
            }
            style={clrBGLightPixelLine}
          />
          <VictoryAxis
            dependentAxis
            tickLabelComponent={
              <VictoryLabel style={{ fontSize: remChartFontSize }} />
            }
            style={clrBGLightPixelLine}
          />
          <VictoryBar
            style={{
              data: {
                fill: ({ datum }) =>
                  datum.volumeIncreased ? clrGreen : clrRed,
              },
            }}
            data={dataToShow}
            x="x"
            y="volume"
          />
        </VictoryChart>
      </Box>
    </div>
  );
};

export default App;
