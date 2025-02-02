import React, { useRef, useEffect, useState } from 'react';
import Plot from 'react-plotly.js';

const sampleData = [
    {
        x: [1, 2, 3],
        y: [2, 6, 3],
        type: 'scatter',
        mode: 'lines+markers',
        marker: {color: 'red'},
    },
];

export default function PlotlyScatterSingle({dataX=[], dataY=[], marker={color: 'blue'}, title='', xAxisTitle='', yAxisTitle=''}) {
    const plotContainer = useRef(null);
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
    //const [ data, setData ] = useState(sampleData);
    const data = [{
        x: dataX,
        y: dataY,
        type: 'scatter',
        mode: 'lines',
        marker: marker
    }];

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


    return (
        <div className="h-full w-full pb-4" ref={plotContainer}>
            <Plot
                data={data}
                layout={{
                    title: title,
                    xaxis: { title: xAxisTitle },
                    yaxis: { title: yAxisTitle },
                    autosize: true,
                    width: dimensions.width,
                    height: dimensions.height,
                    margin: {
                        l: 30,
                        r: 30,
                        t: 30,
                        b: 30,
                    },
                }}
                config={{ responsive: true }}
            />
        </div>
    );
}
