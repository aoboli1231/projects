/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { useState, useEffect } from "react";
import { MapContainer, TileLayer, LayersControl } from "react-leaflet";


import StateElectionLayer from "../politicsMapLayers/StateElectionLayer";
import MapTileLayers from "../MapTileLayers";
import SuburbLgbtLayer from "./map layers/SuburbLgbtLayer";
import StateLgbtLayer from "./map layers/StateLgbtLayer";
import SuburbElectionLayer from "../politicsMapLayers/SuburbElectionLayer";
import IncomeLayer from "../SuburbMapLayers/IncomeLayer";
import EducLayer from "../SuburbMapLayers/EducLayer";

export default function Scenario2Map({ stateData, suburbData }) {
    const mapStyle = {
        height: '100vh',
        width: '100%',
        margin: '0 auto',
    }

    return(
         <div className='container'>
            <div className="">
                <div className="">
                <MapContainer center={[-26.90, 133.76]}
                zoom={5} scrollWheelZoom={true} style={mapStyle}>
                    <MapTileLayers />
                    <LayersControl>
                    { stateData &&
                    <>
                        <StateLgbtLayer data={stateData} checked={true} />
                        <StateElectionLayer data={stateData} />
                    </>
                    }
                    { suburbData &&
                    <>
                        <SuburbLgbtLayer data={suburbData} />
                        <SuburbElectionLayer data={suburbData} />
                        <IncomeLayer data={suburbData} />
                        <EducLayer data={suburbData} />
                    </>
                    }
                    </LayersControl>
                </MapContainer>
                </div>
            </div>
        </div>

    )
}