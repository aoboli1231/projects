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

export default function SuburbLgbtLayer(props) {

    const checked = (props.checked ? true : false)

    const suburbData = props.data

    function getColor(x) {
        const colors = ['#a50026','#d73027','#f46d43','#fdae61','#fee090','#ffffbf',
                        '#e0f3f8','#abd9e9','#74add1','#4575b4','#313695'] //Diverging-Red-Blu

        const range = 0.55
        const mid = 0.5
        const val = (x-mid)/range + 0.5

        const colorIndex = Math.max(0,
            Math.min(colors.length-1,
                Math.floor(colors.length*val))
            )
        
        return colors[colorIndex]
    }

    function getStyle(feature) {
        
        const fillColor = (feature.properties.lgbt_total >= 1) ? 
            getColor(feature.properties["lgbt_pos%"]+feature.properties["lgbt_neu%"]/2) :
            "#000"
        
        const fillOpacity = (feature.properties.lgbt_total >= 1) ? 0.7 : 0

        return {
            fillColor: fillColor,
            weight: 0.3,
            opacity: 0.2,
            color: "black",
            dashArray: '3',
            fillOpacity: fillOpacity
        };
    }

    function round(x, nDecimal) {
        return (Math.round((x + Number.EPSILON) * (10**nDecimal)) / (10**nDecimal))
    }

    function popup(feature, layer) {
        var popUpText = `<div style={text-align: center, margin: 5px}>
            <b>Suburb: ${feature.properties.SAL_NAME21}</b>
            <p><i>Positive %: ${round(feature.properties["lgbt_pos%"]*100, 2)}</i></p>
            <p><i>Neutral %: ${round(feature.properties["lgbt_neu%"]*100, 2)}</i></p>
            <p><i>Negative %: ${round(feature.properties["lgbt_neg%"]*100, 2)}</i></p>
            <p><i>Total: ${feature.properties["lgbt_total"]}</i></p>
            </div>`
        if (feature.properties && (feature.properties["lgbt_total"] != null)) {
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
        <BaseLayer checked={checked} name="Suburb LGBT Sentiment">
            {suburbData && (
                <GeoJSON data={suburbData} key={"lgbt_total"} style={getStyle}
                    onEachFeature={onEachFeature}
                />
            )}
        </BaseLayer>
    )
}