/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
{/* Functions for popUp and styling changes for mouse actions */}

export function overFeature(e, layer) {
    layer.openPopup(e.latlng)

    e.target.setStyle(e.target.feature);

    e.target.bringToFront();
}

export function movePopup(e, layer) {
    layer.openPopup(e.latlng)
}

export function outFeature(e, layer) {
    layer.closePopup()

    e.target.setStyle({
        weight: 2,
        color: 'white',
        dashArray: '3',
        fillOpacity: 0.7
    });

    e.target.bringToFront();
}

export function onEachFeature(feature, layer) {
    popup(feature, layer)

    layer.on({
        mouseover: (e) => overFeature(e, layer),
        mousemove: (e) => movePopup(e, layer),
        mouseout: (e) => outFeature(e, layer)
    })
}