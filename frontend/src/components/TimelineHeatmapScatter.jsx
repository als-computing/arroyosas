import { useState } from "react";
import PlotlyHeatMap from "./PlotlyHeatMap";
import PlotlyScatterMultiple from "./PlotlyScatterMultiple";
import JSONPrinter from "./JSONPrinter";
import InputSlider from "./InputSlider";

import { generateEggData } from "../utils/plotHelper";

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
}

export default function TimelineHeatmapScatter ({arrayData=[], scatterData=[], demo=false}) {
    //to do - see if we can wrap anything in usecallback or usememo since every index change rerenders the plot
    const [ index, setIndex ] = useState(0);

    //handle updates where arrayData goes to 1 and existing large index would be out of bounds
    if (arrayData.length > 0 && index > arrayData.length - 1) setIndex(0);


    if (!demo && arrayData.length > 0 && scatterData.length === arrayData.length) {
        return (
            <div className="w-full h-full flex flex-col px-4">
                <div className="h-[calc(100%-7rem)] max-h-[50rem] w-full flex justify-center">
                    <div className="h-full w-1/2 max-w-[50rem] pt-4 ">
                        <PlotlyHeatMap array={arrayData[index].data} fixPlotHeightToParent={true} title={"frame " + index + " - " + arrayData[index].metadata.timestamp}/>
                    </div>
                    <div className="w-1/2 h-full">
                        <PlotlyScatterMultiple data={[scatterData[index]]}/>
                    </div>
                </div>

                <div className="w-full flex justify-center items-center">
                    <InputSlider width="w-full max-w-xl" max={arrayData.length-1} min={0} onChange={setIndex}  value={index} marks={Array.from({ length: arrayData.length }, (_, i) => i)}/>
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

               