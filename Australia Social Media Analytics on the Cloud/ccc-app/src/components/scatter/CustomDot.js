/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React from "react";

const RADIUS_SELECTED = 6;
const RADIUS_UNSELECTED = 3;
const COLOR_SELECTED = "green";
const COLOR_UNSELECTED = "red";

export default function CustomDot(props) {
  const { cx, cy, payload, selectedPoint } = props;
  const isSelected =
    selectedPoint.x === payload.x && selectedPoint.y === payload.y;

  return (
    <g
      className="custom-dot"
      data-chart-x={cx}
      data-chart-y={cy}
      data-x-value={payload.x}
      data-y-value={payload.y}
      data-radius={isSelected ? RADIUS_UNSELECTED : RADIUS_SELECTED}
    >
      {!isSelected ? (
        <circle cx={cx} cy={cy} r={RADIUS_UNSELECTED} fill={COLOR_UNSELECTED} />
      ) : (
        <>
          <circle
            cx={cx}
            cy={cy}
            r={RADIUS_SELECTED / 2}
            fill={COLOR_SELECTED}
          />
          <circle
            cx={cx}
            cy={cy}
            r={RADIUS_SELECTED}
            fill="none"
            stroke={COLOR_SELECTED}
          />
        </>
      )}
    </g>
  );
}
