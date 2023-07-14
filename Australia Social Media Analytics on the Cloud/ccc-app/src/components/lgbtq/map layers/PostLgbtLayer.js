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

export default function PostLgbtLayer(props) {

    const checked = (props.checked ? true : false)

    const postData = props.data

    function getColor(x) {
        const colors = ['#b2182b','#d6604d','#f4a582','#fddbc7','#d1e5f0','#92c5de','#4393c3','#2166ac'] //Diverging-Red-Blu

        const colorIndex = Math.min(Math.floor(colors.length*(x)), colors.length-1)

        return colors[colorIndex]
    }

    function getStyle(feature) {
        return {
            fillColor: getColor(feature.properties["Positive Proportion"]+feature.properties["Neutral Proportion"]/2),
            weight: 2,
            opacity: 1,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.7
        };
    }

    function round(x, nDecimal) {
        return (Math.round((x + Number.EPSILON) * (10**nDecimal)) / (10**nDecimal))
    }

    function popup(feature, layer) {
        var popUpText = `<div style={text-align: center, margin: 5px}>
            <b>Postcode: ${feature.properties.POA_CODE21}</b>
            <p><i>Positive %: ${round(feature.properties["Positive Proportion"]*100, 2)}</i></p>
            <p><i>Neutral %: ${round(feature.properties["Neutral Proportion"]*100, 2)}</i></p>
            <p><i>Negative %: ${round(feature.properties["Negative Proportion"]*100, 2)}</i></p>
            <p><i>Total: ${feature.properties["Total"]}</i></p>
            </div>`
        if (feature.properties && (feature.properties["Positive Proportion"] != null)) {
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

        e.target.setStyle({
            weight: 2,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.7
        });
    
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
        <BaseLayer checked={checked} name="Postcode LGBT Sentiment">
            {postData && (
                <GeoJSON data={postData} key={"Positive Proportion"} style={getStyle}
                    onEachFeature={onEachFeature}
                />
            )}
        </BaseLayer>
    )
}