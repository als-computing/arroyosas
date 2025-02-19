import { useState, useEffect } from "react";

import Mermaid from "../components/Mermaid";
import InputSlider from "../components/InputSlider";
import PlotlyHeatMap from "../components/PlotlyHeatMap";

import { getSearchResults } from "../utils/tiledAPI";
import { flip2DArray } from "../utils/plotHelper";
const links = [
    "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::1,::1",
    "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=157,::1,::1",
    "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=156,::1,::1",
    "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=155,::1,::1"
]

export default function Services(){
    const [ sliderValue, setSliderValue ] = useState(0);
    const [ plot, setPlot ] = useState([]);

    const fetchData = async (url) => {
        try {
            const result = await getSearchResults(url);
            const flippedArray = flip2DArray(result);
            setPlot(flippedArray); 
        } catch (error) {
            console.error("Error fetching search results:", error);
        }
    };

    const handleSliderChange = (newValue) => {
        fetchData(links[newValue]);
        setSliderValue(newValue);
    }



    useEffect(() => {
        fetchData("http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::1,::1");
    }, [])
    return (
        <div className="w-screen h-screen">
            <div className="w-1/2 h-1/2">
                <PlotlyHeatMap array={plot} fixPlotHeightToParent={true}/>
            </div>
            <div className="w-1/2">
                <InputSlider value={sliderValue} min={0} max={links.length - 1} onChange={handleSliderChange}/>
            </div>
        </div>
    )
}