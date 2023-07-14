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
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine
} from "recharts";

//--Unfinished: Testing more customisable Bar component--//

export default function CustomBar(props) {

    const { data, xAxis, barKeys, barColor=["000"], stackID=null, ...params } = props

    return (
        <div>
            <BarChart
                stackOffset="sign"
                width= {params.width ? params.width : 400}
                height={params.height ? params.height : 300}
                data={data}
                margin={params.margin ? params.margin : {
                    top: 25,
                    right: 20,
                    left: 20,
                    bottom: 10
                }}
            >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey={xAxis} />
                <YAxis />
                <Tooltip />
                <Legend />
                <ReferenceLine y={0} stroke="#000" />
                {barKeys.map((key, index) => 
                    <Bar dataKey={key} 
                        fill={barColor.length===barKeys.length ? barColor[index] : barColor} 
                        stackId={stackID} />)}
            </BarChart>
            
        </div>
  );
}

