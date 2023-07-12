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
import ChartTitle from './ChartTitle';

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
            {payload.lang}
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

export default function PieLangPos() {
    const [activeIndex, setActiveIndex] = useState(0);
    const onPieEnter = useCallback(
      (_, index) => {
        setActiveIndex(index);
      },
      [setActiveIndex]
    );

    const [langSentData, setLangSentData] = useState([])

    const getData = () => {
        fetch('http://localhost:3001/langSent', {
            header:{
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        })
        .then((res) => res.json())
        .then((data) => {setLangSentData(data)})
        .catch(err => {
          console.log('======failed to fetch data=======');
          console.log(err);
      });
    }

    useEffect(() => {
        getData()
    }, [])
  
    return (
      <>
        <ChartTitle title="Positive Sentiment by Language" />
        <PieChart width={400} height={400}>
        <Pie
          activeIndex={activeIndex}
          activeShape={renderActiveShape}
          data={langSentData}
          cx={200}
          cy={200}
          innerRadius={60}
          outerRadius={80}
          fill="#8884d8"
          dataKey="pos"
          onMouseEnter={onPieEnter}
        />
      </PieChart>
      </>
    );
  }
