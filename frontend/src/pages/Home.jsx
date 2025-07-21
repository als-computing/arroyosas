import { useEffect, useState } from 'react';

import Main from "../components/Main";
import Header from "../components/Header";
import Sidebar from "../components/Sidebar";
import SidebarItem from "../components/SidebarItem";
import Widget from "../components/Widget";
import PlotlyHeatMap from "../components/PlotlyHeatMap";
import PlotlyScatterMultiple from "../components/PlotlyScatterMultiple";
import TimelineTiledHeatmapScatter from '../components/TimelineTiledHeatmapScatter';
import Button from "../component_library/Button";
import TextField from "../component_library/TextField";
import ScanMetadata from "../components/ScanMetadata";
import Settings from "../components/Settings";
import FormContainer from "../component_library/FormContainer";
import Status from '../components/Status';
import { phosphorIcons } from '../assets/icons';
import { useGISAXS } from '../hooks/useGISAXS';
export default function Home() {
  const [ isSidebarClosed, setIsSidebarClosed ] = useState(false)

  const {
    messages,
    currentArrayData,
    currentScatterPlot,
    isExperimentRunning,
    isReductionTest,
    wsUrl,
    setWsUrl,
    frameNumber,
    socketStatus,
    socketHistory,
    startWebSocket,
    closeWebSocket,
    heatmapSettings,
    handleHeatmapSettingChange,
    warningMessage,
    metadata,
    linecutData,
    tiledLinks
  } = useGISAXS({});

  var statusMessage;
  if (isExperimentRunning && isReductionTest) {
    statusMessage = "Running Reduction Test";
  } else {
    if (isReductionTest && !isExperimentRunning) {
      statusMessage = "Reduction Test Completed"
    } else {
      if (isExperimentRunning) {
        statusMessage = "Running Scans";
      } else {
        statusMessage = "Inactive"
      }
    }
  }


  //Automatically start the websocket connection on page load
  useEffect(() => {
    startWebSocket();
    return closeWebSocket;
  }, []);

    return (
      <div className="flex-col h-screen w-screen">
        <div className="h-16 shadow-lg">
          <Header isExperimentRunning={isExperimentRunning} showStatus={isSidebarClosed} statusMessage={statusMessage}/>
        </div>

        <div className="flex h-[calc(100vh-4rem)]">
          <Sidebar isSidebarClosed={setIsSidebarClosed}>
            <SidebarItem title="Current Status" icon={phosphorIcons.power}>
              <div className='flex flex-col items-center'>
                <Status height='h-24' slideshow={isExperimentRunning}/>
                <p className="text-sm font-light">{statusMessage}</p>
              </div>
            </SidebarItem>
            <SidebarItem title="Websocket" icon={phosphorIcons.plugsConnected} pulse={socketStatus === 'Open'}>
              <li className="flex flex-col w-full items-center justify-center space-x-6 space-y-4">
                  {warningMessage.length > 0 ? <p className="text-red-500 text-sm">{warningMessage}</p> : ''}
                  <TextField text="Websocket URL" value={wsUrl} cb={setWsUrl} styles='w-64' />
                  {socketStatus === 'closed' 
                    ? 
                      <Button text="Start" cb={startWebSocket}/> 
                    : 
                      socketStatus === "connecting" ?
                        <Button 
                          text={<svg className="mx-3 size-5 animate-spin text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg>} 
                          cb={closeWebSocket}
                        />
                      :
                        <Button text="stop" cb={closeWebSocket}/>
                  }
                  <p className="w-full text-center text-xs text-slate-500">{socketHistory}</p>
              </li>
            </SidebarItem>
            <SidebarItem title='Live Image Settings' icon={phosphorIcons.sliders}>
              <Settings>
                <FormContainer inputs={heatmapSettings} handleInputChange={handleHeatmapSettingChange}/>
              </Settings>
            </SidebarItem>
          </Sidebar>

          <Main >
            <div className="flex flex-wrap justify-around w-full h-full">

              {/* Current Scan */}
              <Widget title={`Most Recent - Frame #${frameNumber}`} width='w-full' defaultHeight='h-1/2'>
                <div className="flex w-full h-full pt-4">
                  <div className="w-1/2 h-full">
                    <PlotlyHeatMap 
                      array={currentArrayData}
                      linecutData={linecutData} 
                      maxHeatmapValue={heatmapSettings.maxHeatmapValue.value}
                      scalePlot={heatmapSettings.scalePlot.value}
                      normalize={heatmapSettings.normalizeArray.value}
                      flipArray={heatmapSettings.flipImg.value} 
                      title='' 
                      xAxisTitle='' 
                      yAxisTitle='' 
                      width='w-full' 
                      fixPlotHeightToParent={true} 
                      showTicks={heatmapSettings.showTicks.value} 
                      tickStep={heatmapSettings.tickStep.value}
                    />              
                  </div>
                  <div className="w-1/2 h-full">
                    <PlotlyScatterMultiple 
                      data={currentScatterPlot} 
                      title='All Plots' 
                      xAxisTitle='x' 
                      yAxisTitle='y'/>                  
                  </div>
                </div>
              </Widget>

              {/* Timeline of All Scans */}
              <Widget title='Timeline' width='w-full' defaultHeight='h-1/2'>
                <TimelineTiledHeatmapScatter 
                  tiledLinks={tiledLinks} 
                  flipArray={heatmapSettings.flipImg.value} 
                  linecutData={linecutData}
                />
              </Widget>

            </div>
          </Main>
        </div>
      </div>

    )
}
