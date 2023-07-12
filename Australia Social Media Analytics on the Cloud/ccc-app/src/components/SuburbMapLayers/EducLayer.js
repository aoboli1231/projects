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
import { round } from "../../functions/round";

export default function EducLayer(props) {

    const checked = (props.checked ? true : false)

    const suburbData = props.data

    function getColor(x) {
        const colors = ['#440154','#482475','#414487','#355f8d','#2a788e','#21918c',
        '#22a884','#44bf70','#7ad151','#bddf26','#fde725'] //Viridis

        const range = 0.6
        const mid = 0.25
        const val = (x-mid)/range + 0.5

        const colorIndex = Math.max(0,
            Math.min(colors.length-1,
                Math.floor(colors.length*val))
            )
        
        return colors[colorIndex]
    }

    function getStyle(feature) {
        
        const fillColor = (feature.properties["uni"] != null) ? 
            getColor(feature.properties["uni"]) :
            "#000"
        
        const fillOpacity = (feature.properties["uni"] != null) ? 0.7 : 0

        return {
            fillColor: fillColor,
            weight: 0.3,
            opacity: 0.2,
            color: "black",
            dashArray: '3',
            fillOpacity: fillOpacity
        };
    }

    function popup(feature, layer) {
        var popUpText = `<div style={text-align: center, margin: 5px}>
            <b>Suburb: ${feature.properties.SAL_NAME21}</b>
            <p><i>Degree %: ${round(feature.properties["uni"]*100, 2)}</i></p>
            </div>`
        if (feature.properties && (feature.properties["uni"] != null)) {
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
        <BaseLayer checked={checked} name="Suburb % with a degree">
            {suburbData && (
                <GeoJSON data={suburbData} key={"uni"} style={getStyle}
                    onEachFeature={onEachFeature}
                />
            )}
        </BaseLayer>
    )
}