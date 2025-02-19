import React, { useRef, useEffect, useState } from 'react';
import Plot from 'react-plotly.js';

const plotlyColorScales = ['Viridis', 'Plasma', 'Inferno', 'Magma', 'Cividis'];

const linecutSample = {
    xStart: 0,
    xEnd: 5,
    yStart: 60,
    yEnd: 70
}

export default function PlotlyHeatMap({
    array = [],
    linecutData=linecutSample,
    linecutThickness = 2,
    title = '',
    xAxisTitle = '',
    yAxisTitle = '',
    colorScale = 'Viridis',
    verticalScaleFactor = 1, // Scale factor for content growth
    width = 'w-full',
    height = 'h-full',
    showTicks = false,
    tickStep = 10,
    fixPlotHeightToParent = false
}) {
    const plotContainer = useRef(null);
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

    // Hook to update dimensions dynamically
    useEffect(() => {
        const resizeObserver = new ResizeObserver((entries) => {
            if (entries[0]) {
                const { width, height } = entries[0].contentRect;
                setDimensions({ width, height });
            }
        });
        if (plotContainer.current) {
            resizeObserver.observe(plotContainer.current);
        }
        return () => resizeObserver.disconnect();
    }, []);

    // Create the heatmap data
    var data = [
        {
            z: array,
            type: 'heatmap',
            colorscale: colorScale,
            zmin: 0,
            zmax: 255,
            showscale: false,
        },
    ];

    // Calculate the y position for the horizontal line
    let lineY = null;
    if (linecutData !== undefined && array.length > 0) {
        lineY = array.length - linecutData.yStart; // Convert from bottom index to y-axis coordinate
    }

    // Calculate the height dynamically based on the number of rows in the array
    const dynamicHeight = Math.max(array.length * verticalScaleFactor, 200); // Minimum height is 200px

    return (
        <div className={`${height} ${width} rounded-b-md pb-6 flex-col content-end relative`} ref={plotContainer}>
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
                    autosize: true,
                    width: dimensions.width,
                    height: fixPlotHeightToParent ? dimensions.height : dynamicHeight, // Dynamically set height
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
                                x0: 0,
                                x1: array[0]?.length - 1 || 1, // Assuming non-empty array, width of heatmap
                                y0: lineY,
                                y1: lineY,
                                line: {
                                    color: 'red',
                                    width: linecutThickness,
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
