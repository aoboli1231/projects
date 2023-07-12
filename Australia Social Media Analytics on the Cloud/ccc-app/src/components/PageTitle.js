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
import { Card, Container, Typography  } from "@mui/material";


export default function PageTitle({title}) {
    return (
            <Card style={{ marginTop: "1em", marginBottom: "1em", background: 'linear-gradient(to right, #d4dadc, #1976D2, #1976D2, #1976D2, #d4dadc)' }}>
                <Typography className="scenario-title" variant="h2" align="center" color="white" style={{ margin: "0.2em" }}>
                    {title}
                </Typography>
            </Card>
    )
}