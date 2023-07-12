/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { useCallback, useState, useEffect } from 'react';
import { PieChart, Pie, Sector } from "recharts";
import ChartTitle from '../ChartTitle';
import { Box, Container, Grid, Typography } from '@mui/material';

const renderActiveShape = (props) => {
    const RADIAN = Math.PI / 180;
    const { cx, cy, midAngle, innerRadius, outerRadius, startAngle, endAngle, fill, payload, percent, value } = props;
    const sin = Math.sin(-RADIAN * midAngle);
    const cos = Math.cos(-RADIAN * midAngle);
    const sx = cx + (outerRadius + 10) * cos;
    const sy = cy + (outerRadius + 10) * sin;
    const mx = cx + (outerRadius + 30) * cos;
    const my = cy + (outerRadius + 30) * sin;
    const ex = mx + (cos >= 0 ? 1 : -1) * 22;
    const ey = my;
    const textAnchor = cos >= 0 ? 'start' : 'end';
  
    return (
        <g>
            <text x={cx} y={cy} dy={8} textAnchor="middle" fill={fill}>
            {payload.sentiment}
            </text>
            <Sector
            cx={cx}
            cy={cy}
            innerRadius={innerRadius}
            outerRadius={outerRadius}
            startAngle={startAngle}
            endAngle={endAngle}
            fill={fill}
            />
            <Sector
            cx={cx}
            cy={cy}
            startAngle={startAngle}
            endAngle={endAngle}
            innerRadius={outerRadius + 6}
            outerRadius={outerRadius + 10}
            fill={fill}
            />
            <path d={`M${sx},${sy}L${mx},${my}L${ex},${ey}`} stroke={fill} fill="none" />
            <circle cx={ex} cy={ey} r={2} fill={fill} stroke="none" />
            <text x={ex + (cos >= 0 ? 1 : -1) * 12} y={ey} textAnchor={textAnchor} fill="#333">{`${value}`}</text>
            <text x={ex + (cos >= 0 ? 1 : -1) * 12} y={ey} dy={18} textAnchor={textAnchor} fill="#999">
            {`(${(percent * 100).toFixed(2)}%)`}
            </text>
        </g>
    );
};

export default function PieTotalSent({ mastProp, twitterProp }) {
    const [activeIndex, setActiveIndex] = useState(0);
    const onPieEnter = useCallback(
      (_, index) => {
        setActiveIndex(index);
      },
      [setActiveIndex]
    );

    const [mastData, setMastData] = useState(null)
    const [twitterData, setTwitterData] = useState(null)

    function formatData(data, setData) {
      var newdata = Object.keys(data).map(key => (
        {"sentiment": key, "count": data[key]}
        ))
      setData(newdata)
    }

    useEffect(() => {
      formatData(mastProp, setMastData)
      formatData(twitterProp, setTwitterData)
    }, [])
  
    return (
      <>
        <ChartTitle title="Overall Sentiment" />
        <PieChart width={800} height={300}>
          {
            twitterData &&
            <Pie
            activeIndex={activeIndex}
            activeShape={renderActiveShape}
            data={twitterData}
            cx={220}
            cy={150}
            innerRadius={60}
            outerRadius={100}
            fill="#03c2fc"
            dataKey="count"
            onMouseEnter={onPieEnter}
          />
          }
          {
            mastData &&
            <Pie
            activeIndex={activeIndex}
            activeShape={renderActiveShape}
            data={mastData}
            cx={530}
            cy={150}
            innerRadius={60}
            outerRadius={100}
            fill="#6734eb"
            dataKey="count"
            onMouseEnter={onPieEnter}
          />
          }
      </PieChart>
      <Typography variant='b1' align='center' color="#03c2fc">Twitter (Left)</Typography>
      <Typography variant='b1' align='center'>   |   </Typography>
      <Typography variant='b1' align='center' color="#6734eb">Mastodon (Right)</Typography>
      </>
    );
  }
