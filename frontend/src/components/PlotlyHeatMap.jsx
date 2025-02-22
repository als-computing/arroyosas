import React, { useRef, useEffect, useState } from 'react';
import { flip2DArray, normalizeArray } from '../utils/plotHelper';
import Plot from 'react-plotly.js';

const plotlyColorScales = ['Viridis', 'Plasma', 'Inferno', 'Magma', 'Cividis'];

const linecutSample = {
    x0: 0,
    x1: 1000,
    y0: 60,
    y1: 70,
    thickness: 1
}

export default function PlotlyHeatMap({
    array = [],
    preserveAspectRatio = true,
    linecutData=null,
    title = '',
    xAxisTitle = '',
    yAxisTitle = '',
    colorScale = 'Viridis',
    verticalScaleFactor = 1, // Scale factor for content growth
    width = 'w-full',
    height = 'h-full',
    showTicks = false,
    tickStep = 10,
    maxHeatmapValue=255,
    scalePlot=true,
    normalize=false,
    fixPlotHeightToParent = false,
    flipArray = false
}) {
    const plotContainer = useRef(null);
    const aspectRatio = useRef(1);
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
    //console.log({array})

    // Hook to update dimensions dynamically
    const resizeObserver = new ResizeObserver((entries) => {
        if (entries[0]) {
            const { width, height } = entries[0].contentRect;
            if (!preserveAspectRatio) {
                setDimensions({width, height});
            } else {
                const containerAR = height / width;
                const arrayAR = aspectRatio.current; //need ref to avoid stale closure
                if (arrayAR > containerAR) {
                    const scaledContainerWidth = height / arrayAR;
                    setDimensions({width: scaledContainerWidth, height: height});
                } else {
                    const scaledContainerHeight = width * arrayAR;
                    setDimensions({width: width, height: scaledContainerHeight});
                }
            }
        }
    });
    useEffect(() => {
        if (plotContainer.current) {
            resizeObserver.observe(plotContainer.current);
        }
        return () => resizeObserver.disconnect();
    }, []);

    useEffect(() => {
        //console.log('array use effect')
        if (array.length > 0) {
            var currentAR = array.length / array[0].length;
            if (aspectRatio.current !== currentAR) {
                //console.log('set new AR')
                aspectRatio.current = currentAR;
            }
        }
    }, [array]);

    var processedData = array;
    if (normalize) {
        processedData = normalizeArray(array, maxHeatmapValue);
    }
    if (flipArray) {
        processedData = flip2DArray(array);
    }

    // Create the heatmap data
    var data = [
        {
            z: processedData,
            type: 'heatmap',
            colorscale: colorScale,
            zmin: 0,
            zmax: scalePlot ? maxHeatmapValue : 255,
            showscale: false,
        },
    ];

    // Calculate the y position for the horizontal line
/*     let lineY = null;
    if (linecutData && array.length > 0) {
        // TODO - verify if this has any issues when flip image is set to true
        lineY = array.length - linecutData.yStart; // Convert from bottom index to y-axis coordinate
    } */

    // Calculate the height dynamically based on the number of rows in the array
    //const dynamicHeight = Math.max(array.length * verticalScaleFactor, 200); // Minimum height is 200px

    return (
        <div className={`${height} ${width} rounded-b-md pb-6 flex items-center justify-center relative`} ref={plotContainer}>
            <Plot
                data={data}
                layout={{
                    title: {
                        text: '',
                    },
                    xaxis: {
                        title: xAxisTitle,
                    },
                    yaxis: {
                        title: yAxisTitle,
                        range: [-0.5, array.length - 0.5], // Dynamically adjust y-axis range
                        autorange: false,
                        tickmode: showTicks ? 'linear' : '', 
                        tick0: 0, 
                        dtick: showTicks ? tickStep : 10000, 
                        showticklabels: showTicks
                    },
                    autosize: false,
                    width: dimensions.width,
                    height: dimensions.height,//fixPlotHeightToParent ? dimensions.height : dynamicHeight, // Dynamically set height
                    margin: {
                        l: showTicks ? 50 : 10,
                        r: 10,
                        t: 0,
                        b: 0,
                    },
                    shapes: linecutData !== null
                        ? [
                            {
                                type: 'line',
                                x0: Math.max(linecutData.x0, 0),
                                x1: Math.min(linecutData.x1, array[0].length - 1), 
                                y0: linecutData.y0, 
                                y1: linecutData.y1,
                                line: {
                                    color: 'red',
                                    width: linecutData.thickness,
                                },
                            },
                        ]
                        : [],
                }}
                config={{ responsive: true }}
                className="rounded-b-md"
            />
            <div className="absolute bottom-0 left-0 right-0 text-center text-md font-semibold">
                {title}
            </div>
        </div>
    );
}
