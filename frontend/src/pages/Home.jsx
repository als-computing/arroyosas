import { useEffect, useState } from 'react';

import Main from "../components/Main";
import Header from "../components/Header";
import Sidebar from "../components/Sidebar";
import SidebarItem from "../components/SidebarItem";
import Widget from "../components/Widget";
import PlotlyHeatMap from "../components/PlotlyHeatMap";
import PlotlyScatterSingle from "../components/PlotlyScatterSingle";
import PlotlyScatterMultiple from "../components/PlotlyScatterMultiple";
import ConsoleViewer from "../components/ConsoleViewer";
import Timeline from '../components/Timeline';
import Button from "../component_library/Button";
import TextField from "../component_library/TextField";
import ScanMetadata from "../components/ScanMetadata";
import Settings from "../components/Settings";
import FormContainer from "../component_library/FormContainer";
import Status from '../components/Status';
import { phosphorIcons } from '../assets/icons';

import { useAPXPS } from "../hooks/useAPXPS";
import { useGISAXS } from '../hooks/useGISAXS';
export default function Home() {
  const [ isSidebarClosed, setIsSidebarClosed ] = useState(false)

  const {
    messages,
    currentArrayData,
    currentScatterPlot,
    cumulativeScatterPlots,
    cumulativeArrayData,
    isExperimentRunning,
    wsUrl,
    setWsUrl,
    frameNumber,
    socketStatus,
    startWebSocket,
    closeWebSocket,
    heatmapSettings,
    handleHeatmapSettingChange,
    warningMessage,
    metadata,
  } = useGISAXS({});

  function generateEggData(size) {
    const maxVal = 255; // Maximum intensity
    const center = size / 2; // Center of the Egg
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
  }
  
  // Example usage
  const size = 10; // Adjust size for desired resolution
  const eggData = generateEggData(size);


  //Automatically start the websocket connection on page load
  useEffect(() => {
    //startWebSocket();
    //return closeWebSocket;
  }, []);

    return (
      <div className="flex-col h-screen w-screen">

        <div className="h-16 shadow-lg">
          <Header isExperimentRunning={isExperimentRunning} showStatus={isSidebarClosed}/>
        </div>

        <div className="flex h-[calc(100vh-4rem)]">
          <Sidebar isSidebarClosed={setIsSidebarClosed}>
            <SidebarItem title="Current Status" icon={phosphorIcons.power}>
              <div className='flex flex-col items-center'>
                <Status height='h-24' slideshow={isExperimentRunning}/>
                <p className="text-sm font-light">Not active</p>
              </div>
            </SidebarItem>
            <SidebarItem title="Websocket" icon={phosphorIcons.plugsConnected} pulse={socketStatus === 'Open'}>
              <li className="flex flex-col w-full items-center justify-center space-x-6 space-y-4">
                  {warningMessage.length > 0 ? <p className="text-red-500 text-lg">{warningMessage}</p> : ''}
                  <TextField text="Websocket URL" value={wsUrl} cb={setWsUrl} styles='w-64' />
                  {socketStatus === 'closed' ? <Button text="Start" cb={startWebSocket}/> : <Button text="stop" cb={closeWebSocket}/>}
              </li>
            </SidebarItem>
            <SidebarItem title='Live Image Settings' icon={phosphorIcons.sliders}>
              <Settings>
                <FormContainer inputs={heatmapSettings} handleInputChange={handleHeatmapSettingChange}/>
              </Settings>
            </SidebarItem>
{/*             <SidebarItem title='Scan Metadata' icon={phosphorIcons.fileMd}>
              <pre className="text-sm font-mono text-gray-700 whitespace-pre-wrap break-words pl-4">{JSON.stringify(metadata, null, 2)}</pre>
            </SidebarItem> */}
          </Sidebar>

          <Main >
            <div className="flex flex-wrap justify-around w-full h-full">
              <Widget title={`Current Frame #${frameNumber}`} width='w-1/2' minWidth="min-w-96" maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[800px]' expandedWidth='w-full'>
                <PlotlyHeatMap array={currentArrayData} title='Most Recent' xAxisTitle='' yAxisTitle='' width='w-full' fixPlotHeightToParent={true} showTicks={heatmapSettings.showTicks.value} tickStep={heatmapSettings.tickStep.value}/>
              </Widget>
              <Widget title={`Scan Timeline`} width='w-1/2' minWidth="min-w-96" maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[80%]' expandedWidth='w-full'>
                <Timeline cumulativeArrayData={cumulativeArrayData}/>
              </Widget>
              <Widget title='All 1D Plots' width='w-full' defaultHeight='h-1/2'>
                  <PlotlyScatterMultiple data={cumulativeScatterPlots} title='All Plots' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
              <Widget title='1D Plots' width='w-full' defaultHeight='h-1/2'>
                  <PlotlyScatterMultiple data={currentScatterPlot} title='Most Recent Plot' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
            </div>
          </Main>
        </div>
      </div>

    )
}
