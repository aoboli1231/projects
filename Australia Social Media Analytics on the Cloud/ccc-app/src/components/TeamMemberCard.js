/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import { Card, CardContent, CardMedia, Typography } from "@mui/material";
import React from "react";


export default function TeamMemberCard({ img, firstName, lastName }) {
    return (
        <Card >
            <div
                style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center"
                }}
            >
                <CardMedia
                    component="img"
                    image={img}
                    sx={{
                        borderRadius: '50%',
                        width: "8em",
                        height: "8em",
                        margin: "1em"
                    }}
                />
            </div>
            <CardContent>
                <Typography variant="h6" color="text.primary" align="center">
                    {firstName}
                </Typography>
                <Typography variant="h6" color="text.secondary" align="center">
                    {lastName}
                </Typography>
            </CardContent>
        </Card>
    )
}