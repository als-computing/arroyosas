import { useState } from "react";
import PlotlyHeatMap from "./PlotlyHeatMap";
import JSONPrinter from "./JSONPrinter";
import InputSlider from "./InputSlider";

import { generateEggData } from "../utils/plotHelper";

export default function Timeline ({cumulativeArrayData=[]}) {
    const [ index, setIndex ] = useState(0);
    var sampleCumulativeData = [];
    const samples = 7;
    for (let i=0; i<samples; i++) {
        sampleCumulativeData.push(generateEggData(16, 200+i*8, i+0.1));
    }
    console.log({sampleCumulativeData})
    if (cumulativeArrayData.length > 0) {
        console.log('return')
        return (
            <div>
                <PlotlyHeatMap data={cumulativeArrayData[index].data}/>
                <JSONPrinter JSON={cumulativeArrayData[index].metadata} />
            </div>
        )
    } else {
        return (
            <div className="w-full h-full flex flex-col px-4">
                <div className="h-[calc(100%-7rem)] max-h-[50rem] w-full flex justify-center">
                    <div className="h-full w-full max-w-[50rem] pt-4 ">
                        <PlotlyHeatMap array={sampleCumulativeData[index]} fixPlotHeightToParent={true} title={"frame " + index}/>
                    </div>
                </div>

                <div className="w-full flex justify-center items-center">
                    <InputSlider width="w-full max-w-xl" max={samples-1} min={0} onChange={setIndex}  value={index} marks={Array.from({ length: samples }, (_, i) => i)}/>
                </div>

            </div>
        )
    }
}

               