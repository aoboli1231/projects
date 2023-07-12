/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import "./scatter.css";
import React, { useEffect, useState } from "react";
import { ScatterChart, Scatter, ReferenceArea, XAxis, YAxis } from "recharts";
import CustomDot from "./CustomDot";
import getClickedPoint from "./getClickedPoint";

/*const data = [
  { x: 50, y: 200 },
  { x: 70, y: 100 },
  { x: 170, y: 300 },
  { x: 140, y: 250 },
  { x: 150, y: 400 },
  { x: 110, y: 280 }
];
*/
export default function ScatterZoom({ suburbData }) {
  const [highUniData, setHighUniData] = useState(null)
  const [lowUniData, setLowUniData] = useState(null)
  const [data, setData] = useState(null)

  function formatData(suburbData) {
    var highUni = []
    var lowUni = []

    let i = 0
    let j = 0

    suburbData['features'].forEach(suburb => {
      if ((suburb['properties']['SAL_NAME21'] != undefined) && (suburb['properties']['war_neg%'] != undefined) && 
        (suburb['properties']['median_income'] != undefined) && (suburb['properties']['war_total'] >= 5)) {
        
        var name = suburb['properties']['SAL_NAME21'];
        var pos_prop = suburb['properties']['war_neg%'];
        var median_income = suburb['properties']['median_income'];
        if (suburb['properties']['uni'] >= 0.25) {
          highUni.push({'suburb': name, 'war_neg%': pos_prop, 'median_income': median_income});
          i += 1;
        } else if (suburb['properties']['uni'] < 0.25) {
          lowUni.push({'suburb': name, 'war_neg%': pos_prop, 'median_income': median_income});
          j += 1;
        }
      }
        //console.log(subInfo)
    })

    setHighUniData(highUni)
    setLowUniData(lowUni)
    setData(highUni)

    console.log("i", i)
    console.log("j", i)
  }

  useEffect(() => {
    formatData(data)
  }, [])

  const MIN_ZOOM = 5; // adjust based on your data
  const DEFAULT_ZOOM = { x1: null, y1: null, x2: null, y2: null };
  // data currently on the plot
  const [filteredData, setFilteredData] = useState(data);
  // selected data point
  const [selectedPoint, setSelectedPoint] = useState(data[1]);
  // zoom coordinates
  const [zoomArea, setZoomArea] = useState(DEFAULT_ZOOM);
  // flag if currently zooming (press and drag)
  const [isZooming, setIsZooming] = useState(false);
  // flag if zoomed in
  const isZoomed = filteredData?.length !== data?.length;

  // flag to show the zooming area (ReferenceArea)
  const showZoomBox =
    isZooming &&
    !(Math.abs(zoomArea.x1 - zoomArea.x2) < MIN_ZOOM) &&
    !(Math.abs(zoomArea.y1 - zoomArea.y2) < MIN_ZOOM);

  // reset the states on zoom out
  function handleZoomOut() {
    setFilteredData(data);
    setZoomArea(DEFAULT_ZOOM);
  }

  /**
   * Two possible events:
   * 1. Clicking on a dot(data point) to select
   * 2. Clicking on the plot to start zooming
   */
  function handleMouseDown(e) {
    setIsZooming(true);
    const { chartX, chartY, xValue, yValue } = e || {};
    const clickedPoint = getClickedPoint(chartX, chartY, filteredData);

    if (clickedPoint) {
      setSelectedPoint(clickedPoint);
    } else {
      // console.log("zoom start");
      setZoomArea({ x1: xValue, y1: yValue, x2: xValue, y2: yValue });
    }
  }

  // Update zoom end coordinates
  function handleMouseMove(e) {
    if (isZooming) {
      // console.log("zoom selecting");
      setZoomArea((prev) => ({ ...prev, x2: e?.xValue, y2: e?.yValue }));
    }
  }

  // When zooming stops, update with filtered data points
  // Ignore if not enough zoom
  function handleMouseUp(e) {
    if (isZooming) {
      setIsZooming(false);
      let { x1, y1, x2, y2 } = zoomArea;

      // ensure x1 <= x2 and y1 <= y2
      if (x1 > x2) [x1, x2] = [x2, x1];
      if (y1 > y2) [y1, y2] = [y2, y1];

      if (x2 - x1 < MIN_ZOOM || y2 - y1 < MIN_ZOOM) {
        // console.log("zoom cancel");
      } else {
        // console.log("zoom stop");
        const dataPointsInRange = filteredData.filter(
          (d) => d.x >= x1 && d.x <= x2 && d.y >= y1 && d.y <= y2
        );
        setFilteredData(dataPointsInRange);
        setZoomArea(DEFAULT_ZOOM);
      }
    }
  }

  return (
    <div className="plot-container">
      {isZoomed && <button onClick={handleZoomOut}>Zoom Out</button>}
      <ScatterChart
        width={400}
        height={400}
        margin={{ top: 50 }}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
      >
        <XAxis
          type="number"
          dataKey="median_income"
          domain={["dataMin - 20", "dataMax + 20"]}
        />
        <YAxis
          type="number"
          dataKey="war_pos%"
          domain={["dataMin - 50", "dataMax + 50"]}
        />
        {showZoomBox && (
          <ReferenceArea
            x1={zoomArea?.x1}
            x2={zoomArea?.x2}
            y1={zoomArea?.y1}
            y2={zoomArea?.y2}
          />
        )}
        <Scatter
          data={filteredData}
          shape={<CustomDot selectedPoint={selectedPoint} />}
        />
      </ScatterChart>
    </div>
  );
}
