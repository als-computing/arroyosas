import { useState, useEffect } from "react";
import PlotlyHeatMap from "./PlotlyHeatMap";
import PlotlyScatterMultiple from "./PlotlyScatterMultiple";
import JSONPrinter from "./JSONPrinter";
import InputSlider from "./InputSlider";
import { getSearchResults, getTableData } from "../utils/tiledAPI";
import { generateEggData, flip2DArray } from "../utils/plotHelper";

const sampleData = [
    {
        x: [1, 2, 3],
        y: [2, 6, 3],
        type: 'scatter',
        mode: 'lines+markers',
        marker: {color: 'red'},
    },
];

const generateSampleScatterPlots = (n=1) => {
    var scatterPlots = [];
    for (let i = 0; i < n; i++) {
        scatterPlots.push({
            x: [1, 2, 3],
            y: [2*i, 6*i - 2*i*i, 3*i],
            type: 'scatter',
            mode: 'lines+markers',
            marker: {color: 'red'},
        });
    }
    return scatterPlots;
};


export default function TimelineTiledHeatmapScatter ({tiledLinks=[], demo=false, flipArray=true, linecutData=null}) {
    const [ index, setIndex ] = useState(0);
    const [ heatmapArray, setHeatmapArray ] = useState(null);
    const [ scatterPlot, setScatterPlot ] = useState(null);

    const fetchData = async (url) => {
        try {
            const result = await getSearchResults(url);
            return result;
        } catch (error) {
            console.error("Error fetching search results:", error);
        }
    };

    const getNewHeatmap = async (url) => {
        const rawArray = await fetchData(url);
        if (!rawArray) return;
        setHeatmapArray(rawArray);
    };

    const getNewScatterPlot = async (url) => {
        console.log('getting new scatter plot from url:', url);
        const rawArray = await getTableData(url);
        if (!rawArray) return;
        console.log({rawArray}); // Now it's an array of objects
        // [{ A: 0.5699, B: 1.1398, C: 1.7098 }, ...]
        var xValues = [];
        var yValues = [];

        if (rawArray.length > 1) {
            //[x0: float, y0: float], [x1: float, y1: float], [x2: float, y2: float]
            //we don't know what the key names are for x and y values, so we assume that x is the first key and y is the second
            const firstKey = Object.keys(rawArray[0])[0];
            const secondKey = Object.keys(rawArray[0])[1];
            //check in case the first key is actually y and they're swapped
            let xKey = firstKey;
            let yKey = secondKey;
            if (firstKey.includes("y") || firstKey.includes("Y") || secondKey.includes("x") || secondKey.includes("X")) {
                xKey = secondKey;
                yKey = firstKey;
            }
            for (let i=0; i < rawArray.length; i++) {
                xValues.push(rawArray[i][xKey]);
                yValues.push(rawArray[i][yKey]);
            }
        } else {
            console.log('issue processing the scatter plot from tiled, could not resolve dimensions into X and Y');
            console.log({rawArray});
        }
        console.log('processed scatter plot:', {xValues, yValues});

        if (xValues.length > 0 && yValues.length > 0) {
            const newScatterPlot = { 
                x: xValues,
                y: yValues,
                type: 'scatter',
                mode: 'lines+markers',
                marker: {color: 'red'}, 
            };
            setScatterPlot([newScatterPlot]);
        }
    };

    const handleSliderChange = (newIndex) => {
        tiledLinks[newIndex]?.image && getNewHeatmap(tiledLinks[newIndex].image);
        getNewScatterPlot(tiledLinks[newIndex].curve);
        setIndex(newIndex);
    }

    //handle updates where new scan causes tiledLinks goes to 1 and existing large index would be out of bounds
    if (tiledLinks.length > 0 && index > tiledLinks.length - 1) setIndex(0);

    //initialize results on first link
    
    useEffect(() => {   
        if (tiledLinks.length === 1) (handleSliderChange(0));
    }, [tiledLinks]);



    if (!demo && tiledLinks.length > 0 ) {
        return (
            <div className="w-full h-full flex flex-col px-4">
                <div className="h-[calc(100%-7rem)] max-h-[50rem] w-full flex justify-center">
                    <div className="h-full w-1/2 max-w-[50rem] pt-4 ">
                        {heatmapArray &&
                            <PlotlyHeatMap
                                array={heatmapArray} 
                                fixPlotHeightToParent={true} 
                                title={"frame " + index + " - " + tiledLinks[index].timestamp} 
                                flipArray={flipArray}
                                linecutData={linecutData}
                            />
                        }
                    </div>
                    <div className="w-1/2 h-full">
                        <PlotlyScatterMultiple data={scatterPlot} />
                    </div>
                </div>

                <div className="w-full flex justify-center items-center">
                    <InputSlider width="w-full max-w-xl" max={tiledLinks.length-1} min={0} onChange={handleSliderChange}  value={index} marks={Array.from({ length: tiledLinks.length }, (_, i) => i)}/>
                </div>

            </div>
        )
    } else {
        if (!demo) {
            return <p>Waiting for data</p>
        }
    }
    
    if (demo) {
        var sampleCumulativeData = [];
        const samples = 7;
        for (let i=0; i<samples; i++) {
            sampleCumulativeData.push(generateEggData(16, 200+i*8, i+0.1));
        }
        var sampleScatterData = generateSampleScatterPlots(samples);
        return (
            <div className="w-full h-full flex flex-col px-4">
                <div className="h-[calc(100%-7rem)] max-h-[50rem] w-full flex justify-center">
                    <div className="h-full w-1/2 max-w-[50rem] pt-4 ">
                        <PlotlyHeatMap array={sampleCumulativeData[index]} fixPlotHeightToParent={true} title={"frame " + index}/>
                    </div>
                    <div className="w-1/2 h-full">
                        <PlotlyScatterMultiple data={[sampleScatterData[index]]}/>
                    </div>
                </div>

                <div className="w-full flex justify-center items-center">
                    <InputSlider width="w-full max-w-xl" max={samples-1} min={0} onChange={setIndex}  value={index} marks={Array.from({ length: samples }, (_, i) => i)}/>
                </div>

            </div>
        )
    }
    
}
