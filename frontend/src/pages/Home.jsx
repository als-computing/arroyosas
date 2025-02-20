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
import TimelineHeatmap from '../components/TimelineHeatmap';
import TimelineHeatmapScatter from '../components/TimelineHeatmapScatter';
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
    isReductionTest,
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
    linecutData
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
          </Sidebar>

          <Main >
            <div className="flex flex-wrap justify-around w-full h-full">

              {/* Current Scan */}
              <Widget title='Most Recent' width='w-full' defaultHeight='h-1/2'>
                <div className="flex w-full h-full pt-4">
                  <div className="w-1/2 h-full">
                    <PlotlyHeatMap 
                      array={currentArrayData}
                      linecutData={linecutData} 
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
                  <div className="w-1/2 h-fulkl">
                    <PlotlyScatterMultiple data={currentScatterPlot} title='All Plots' xAxisTitle='x' yAxisTitle='y'/>                  
                  </div>
                </div>
              </Widget>

              {/* Timeline of All Scans */}
              <Widget title='Timeline' width='w-full' defaultHeight='h-1/2'>
                <TimelineHeatmapScatter arrayData={cumulativeArrayData} scatterData={cumulativeScatterPlots}/>
              </Widget>



{/* 
              <Widget title='Timeline' width='w-full' defaultHeight='h-1/2'>
                <TimelineHeatmapScatter demo={true}/>
              </Widget>

              <Widget 
                title={isReductionTest ? 'Reduction Test Results' : `Frame #${frameNumber}`} 
                width='w-1/2' minWidth="min-w-96" maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[800px]' expandedWidth='w-full'>
                <PlotlyHeatMap array={currentArrayData} title='Most Recent Scan' xAxisTitle='' yAxisTitle='' width='w-full' fixPlotHeightToParent={true} showTicks={heatmapSettings.showTicks.value} tickStep={heatmapSettings.tickStep.value}/>
              </Widget>
              <Widget title={`Scan Timeline`} width='w-1/2' minWidth="min-w-96" maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[80%]' expandedWidth='w-full'>
                <TimelineHeatmap cumulativeArrayData={cumulativeArrayData}/>
              </Widget>
              <Widget title='All 1D Plots' width='w-full' defaultHeight='h-1/2'>
                  <PlotlyScatterMultiple data={cumulativeScatterPlots} title='All Plots' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
              <Widget title='1D Plots' width='w-full' defaultHeight='h-1/2'>
                  <PlotlyScatterMultiple data={currentScatterPlot} title='Most Recent Plot' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
              <Widget title={`Scan Timeline Demo`} width='w-1/2' minWidth="min-w-96" maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[80%]' expandedWidth='w-full'>
                <TimelineHeatmap demo={true}/>
              </Widget> */}
            </div>
          </Main>
        </div>
      </div>

    )
}
