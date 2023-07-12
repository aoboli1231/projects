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
  ResponsiveContainer,
  Tooltip,
  Label,
  LabelList
} from "recharts";

const renderCustomizedLabel = (props) => {
  const { content, ...rest } = props;

  return <Label {...rest} fontSize="12" fill="#FFFFFF" fontWeight="Bold" />;
};

export class DashboardView extends React.Component {
  render() {
    const data = [
      { name: "NE Send", completed: 230, failed: 335, inprogress: 453 },
      { name: "NE Resend", completed: 335, failed: 330, inprogress: 345 },
      {
        name: "Miles Orchestrator",
        completed: 537,
        failed: 243,
        inprogress: 2110
      },
      {
        name: "Commissions Payment Orch",
        completed: 132,
        failed: 328,
        inprogress: 540
      },
      {
        name: "Business Integrators",
        completed: 530,
        failed: 145,
        inprogress: 335
      },
      { name: "SmartTrack", completed: 538, failed: 312, inprogress: 110 }
    ];

    return (
      <div className="content c-white">
        <h1>Dashboard</h1>
        <ResponsiveContainer height={250} width={"100%"}>
          <BarChart
            layout="vertical"
            data={data}
            margin={{ left: 50, right: 50 }}
            stackOffset="expand"
          >
            <XAxis hide type="number" />
            <YAxis
              type="category"
              dataKey="name"
              stroke="#FFFFFF"
              fontSize="12"
            />
            <Tooltip />
            <Bar dataKey="failed" fill="#dd7876" stackId="a">
              <LabelList
                dataKey="failed"
                position="center"
                content={renderCustomizedLabel}
              />
            </Bar>
            <Bar dataKey="completed" fill="#82ba7f" stackId="a">
              <LabelList
                dataKey="completed"
                position="center"
                content={renderCustomizedLabel}
              />
            </Bar>
            <Bar dataKey="inprogress" fill="#76a8dd" stackId="a">
              <LabelList
                dataKey="inprogress"
                position="center"
                content={renderCustomizedLabel}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
