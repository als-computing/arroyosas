import { useState } from "react";
import PlotlyHeatMap from "./PlotlyHeatMap";
import PlotlyScatterMultiple from "./PlotlyScatterMultiple";
import JSONPrinter from "./JSONPrinter";
import InputSlider from "./InputSlider";
import { getSearchResults } from "../utils/tiledAPI";
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


export default function TimelineTiledHeatmapScatter ({tiledLinks=[], demo=false, flipArray=true}) {
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
        const rawArray = await fetchData(url);
        if (!rawArray) return;
        // we don't know what the scatter plot array looks like
        // but we could assume there is 2 dimensions for now...
        var xValues = [];
        var yValues = [];
        if (rawArray.length === 2) {
            // [x1, x2, x3....], [y1, y2, y3, ....]
            xValues = rawArray[0]; //assume that the first index is X values. it could be switched though.
            yValues = rawArray[1];
        } else {
            if (rawArray.length > 1 && rawArray[0].length === 2) {
                //[x0, y0], [x1, y1], [x2, y2]
                for (let i=0; i < rawArray.length; i++) {
                    xValues.push(rawArray[i][0]); //assume x is in position 0. it could be switched though.
                    yValues.push(rawArray[i][1]);
                }
            } else {
                console.log('issue processing the scatter plot from tiled, could not resolve dimensions into X and Y');
                console.log({rawArray});
            }
        }

        if (xValues.length > 0 && yValues.length > 0) {
            const newScatterPlot = { 
                x: xValues,
                y: yValues,
                type: 'scatter',
                mode: 'lines+markers',
                marker: {color: 'red'}, 
            };
            setScatterPlot(newScatterPlot);
        }
    };

    const handleSliderChange = (newIndex) => {
        getNewHeatmap(tiledLinks[newIndex].image);
        getNewScatterPlot(tiledLinks[newIndex].curve);
        setIndex(newIndex);
    }

    //handle updates where new scan causes tiledLinks goes to 1 and existing large index would be out of bounds
    if (tiledLinks.length > 0 && index > tiledLinks.length - 1) setIndex(0);


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
