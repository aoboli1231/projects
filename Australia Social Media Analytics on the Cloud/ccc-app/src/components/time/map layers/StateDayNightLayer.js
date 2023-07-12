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
import { GeoJSON, LayersControl } from "react-leaflet";
import { geoJSON } from "leaflet";
import { round } from "../../../functions/round";

export default function StateDayNightLayer(props) {

    const checked = (props.checked ? true : false)

    const stateData = props.data

    console.log('stateData', stateData)

    function getColor(x) {
        const colors = ['#440154','#482475','#414487','#355f8d','#2a788e','#21918c',
        '#22a884','#44bf70','#7ad151','#bddf26','#fde725'] //Viridis

        const range = 0.7
        const mid = 0.5
        const val = (x-mid)/range + 0.5

        const colorIndex = Math.max(0,
            Math.min(colors.length-1,
                Math.floor(colors.length*val))
            )
        
        return colors[colorIndex]
    }

    function getStyle(feature) {

        var total = feature.properties["night"]+feature.properties["day"]
        
        const fillColor = (feature.properties.war_total >= 1) ? 
            getColor(feature.properties["night"]/total) :
            "#000"
        
        const fillOpacity = (total >= 1) ? 0.7 : 0

        return {
            fillColor: fillColor,
            weight: 1,
            opacity: 1,
            color: "white",
            dashArray: '3',
            fillOpacity: fillOpacity
        };
    }

    function popup(feature, layer) {
        let total = feature.properties["night"]+feature.properties["day"]
        var popUpText = `<div style={text-align: center, margin: 5px}>
            <b>State: ${feature.properties.STE_NAME21}</b>
            <p><i>Day: ${round(feature.properties["day"], 0)}</i></p>
            <p><i>Night: ${round(feature.properties["night"], 0)}</i></p>
            <p><i>Relative Night %: ${(round(feature.properties["night"]/total)*100, 2)}</i></p>
            <p><i>Total: ${total}</i></p>
            </div>`
        if (feature.properties && (total >= 1)) {
            layer.bindPopup(popUpText);
        }
    }

    function overFeature(e, layer) {
        layer.openPopup(e.latlng)

        e.target.setStyle(e.target.feature);
    
        e.target.bringToFront();
    }

    function movePopup(e, layer) {
        layer.openPopup(e.latlng)
    }

    function outFeature(e, layer) {
        layer.closePopup()
    
        e.target.bringToFront();
    }

    function onEachFeature(feature, layer) {
        popup(feature, layer)

        layer.on({
            mouseover: (e) => overFeature(e, layer),
            mousemove: (e) => movePopup(e, layer),
            mouseout: (e) => outFeature(e, layer)
        })
    }

    const { BaseLayer } = LayersControl

    return(
        <BaseLayer checked={checked} name="State Relative Night Twitter Activity">
            {stateData && (
                <GeoJSON data={stateData} key={"night"} style={getStyle}
                    onEachFeature={onEachFeature}
                />
            )}
        </BaseLayer>
    )
}