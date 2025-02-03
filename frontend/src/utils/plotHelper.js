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

export const processAndDownsampleArrayData = (data = [], width, height, scaleFactor = 1, cb) => {
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
    cb(newData);
};