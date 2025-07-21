const sampleScatterData = [
    {
        x: [1, 2, 3],
        y: [2, 6, 3],
        type: 'scatter',
        mode: 'lines+markers',
        marker: {color: 'red'},
    },
];

export const process1DArray = (array=[], frameNumber='N/A') => {
    try{
        if (array.length > 0) {
            var xValues = [];
            var yValues = [];
            for (let i=0; i<array.length; i++) {
                xValues.push(i);
                yValues.push(array[i]);
            }
            const newPlot = [
                {
                    x: xValues,
                    y: yValues,
                    type: 'scatter',
                    mode: 'lines+markers',
                    marker: {color: 'red'},
                    name: `frame ${frameNumber}`
                }
            ]
            return newPlot;
        } else {
            console.log('Received invalid data type in 1D array processor:');
            console.log({array});
            return false;
        }
    } catch(e) {
        console.error('Received bad 1D array data:', e);
        return false;
    }
}

//the new curve for 2025 July SMI Beamtime will send a 2D array of [ [x1, y1], [x2, y2], ... ]:
let sample2DArray = [
    [1, 3],
    [2, 4],
    [3, 5],
]
export const process2DArray = (array=[], frameNumber='N/A') => {
    try {
        if (array.length > 0 && Array.isArray(array[0]) && array[0].length === 2) {
            var xValues = [];
            var yValues = [];
            for (let i=0; i<array.length; i++) {
                xValues.push(array[i][0]);
                yValues.push(array[i][1]);
            }
            const newPlot = [
                {
                    x: xValues,
                    y: yValues,
                    type: 'scatter',
                    mode: 'lines+markers',
                    marker: {color: 'blue'},
                    name: `frame ${frameNumber}`
                }
            ]
            return newPlot;
        } else {
            console.log('Received invalid data type in 2D array processor:');
            console.log({array});
            return false;
        }
    } catch(e) {
        console.error('Received bad 2D array data:', e);
        return false;
    }
}

export const processJSONPlot = (rawJSONData, frameNumber='N/A') => {
    //receives a json from
    //"1D": message.one_d_reduction.df.to_json(),
    //one_d_reduction=DataFrameModel(df=one_d_reduction),
    const parsedData = JSON.parse(rawJSONData);
    //console.log({parsedData})
    //we receive an array of 'q' and an array of 'qy'

    var xValues = [];
    var yValues = [];
    for (var key in parsedData) {
        xValues.push(key);
        yValues.push(parsedData[key]['0'])
    }
    const newPlot = [
        {
            x: xValues,
            y: yValues,
            type: 'scatter',
            mode: 'lines+markers',
            marker: {color: 'red'},
            name: `frame ${frameNumber}`
        }
    ]
    return newPlot;
}

export const processPeakData = (peakDataArray=[{x:0, h:0, fwhm: 0}], singlePlotCallback=()=>{}, multiPlotCallback=()=>{}) => {

    var recentPlots = [];
    peakDataArray.forEach(data => {
        //receives an array of objects
        var y_peak = data.h;
        var x_peak = data.x;

        // Calculate sigma and define x range
        var sigma = data.fwhm / (2 * Math.sqrt(2 * Math.log(2)));
        var x_min = x_peak - 5 * sigma;
        var x_max = x_peak + 5 * sigma;
        var step = (x_max - x_min) / 100;

        // Generate x and y values for the single plot
        var xValues = [];
        var yValues = [];
        for (let x = x_min; x <= x_max; x += step) {
            var y = y_peak * Math.exp(-Math.pow(x - x_peak, 2) / (2 * Math.pow(sigma, 2)));
            xValues.push(x);
            yValues.push(y);
        }

        // Create single plot object
        recentPlots.push({ x: xValues, y: yValues, type: 'scatter', mode: 'lines' });
    })


    //update state
    singlePlotCallback(recentPlots);
    multiPlotCallback(recentPlots);
};

export const updateCumulativePlot = (recentPlots=[], cb=()=>{}) => {
    //console.log({frameNumber})
   cb((data) => {
     var oldArrayData = Array.from(data);
     var newArrayData = [];
     let totalFrames = oldArrayData.length;
     let colorNumber = 255; //the lightest color for the oldest entries

     //TO DO: refactor this if its slowing the app down
     oldArrayData.forEach((plot, index) => {
         let colorWeight = (totalFrames - index) / totalFrames * colorNumber; //scale color based on index relative to total frames
         plot.line = {
             color: `rgb(${colorWeight}, ${colorWeight}, ${colorWeight})`,
             width: 1,
         };
         plot.mode = 'lines';
         newArrayData.push(plot);
     })
     var newestData = [];
     recentPlots.forEach((plot) => {
         var newPlot = {
             x: plot.x,
             y: plot.y,
             line: {
                 color: 'rgb(0, 94, 245)',
                 width: 2,
             },
             name: plot.name,
             mode: 'lines+markers'
         };
         newestData.push(newPlot);
     })
     return [...newArrayData, ...newestData];
   })
 };

export const processAndDownsampleArrayData = (data = [], width, height, scaleFactor = 1) => {
    if (scaleFactor < 1) throw new Error("Scale factor must be 1 or greater.");

    const downsampledHeight = Math.floor(height / scaleFactor);
    const downsampledWidth = Math.floor(width / scaleFactor);
    const newData = [];

    for (let row = 0; row < downsampledHeight; row++) {
        const newRow = [];
        for (let col = 0; col < downsampledWidth; col++) {
            let sum = 0;
            let count = 0;

            // Sum up values within the scaleFactor x scaleFactor block
            for (let i = 0; i < scaleFactor; i++) {
                for (let j = 0; j < scaleFactor; j++) {
                    const originalRow = row * scaleFactor + i;
                    const originalCol = col * scaleFactor + j;
                    const index = originalRow * width + originalCol;

                    if (originalRow < height && originalCol < width) {
                        sum += data[index];
                        count++;
                    }
                }
            }
            // Calculate the average value and add to the downsampled row
            newRow.push(sum / count);
        }
        newData.push(newRow);
    }
    //cb(newData);
    return newData;
};

export const flip2DArray = (array) => {
    //flips an array about the horizontal axis
    let newFlippedArray = [];
    for (let i=array.length-1; i>=0; i--) {
        newFlippedArray.push(array[i]);
    }
    return newFlippedArray;
}

export  function generateEggData(size, maxVal=255, offset=2) {
    const center = size / offset; // Center of the Egg
    const data = [];
  
    for (let y = 0; y < size; y++) {
      const row = [];
      for (let x = 0; x < size; x++) {
        // Calculate distance from the center
        const dx = x - center;
        const dy = y - center;
        const distance = Math.sqrt(dx * dx + dy * dy);
  
        // Egg-like function: exponential decay from center
        const intensity = maxVal * Math.exp(-distance * distance / (2 * (center / 2) ** 2));
        row.push(Math.round(intensity)); // Normalize to integer
      }
      data.push(row);
    }
    return data;
};

export const normalizeArray = (arr, maxLimit) => {
    if (arr.length === 0) return arr;
    var min=1000000;
    var max=-1;
    var sum=0;
    for (let i=0; i<arr.length; i++) {
        if (arr[i] < min) {
            min = arr[i];
        }
        if (arr[i] > max) {
            max = arr[i];
        }
        sum = sum+ arr[i];
    }
    var mean = sum / arr.length;

    if (max === min) return arr; // Avoid divide by zero

    return arr.map(row => row.map(value => 
        ((value - min) / (max - min)) * maxLimit
    ));
};