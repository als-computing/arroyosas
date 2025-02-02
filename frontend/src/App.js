import { useEffect } from 'react';

import Main from "./components/Main";
import Header from "./components/Header";
import Sidebar from "./components/Sidebar";
import SidebarItem from "./components/SidebarItem";
import Widget from "./components/Widget";
import PlotlyHeatMap from "./components/PlotlyHeatMap";
import PlotlyScatterSingle from "./components/PlotlyScatterSingle";
import PlotlyScatterMultiple from "./components/PlotlyScatterMultiple";
import ConsoleViewer from "./components/ConsoleViewer";
import Button from "./component_library/Button";
import TextField from "./component_library/TextField";
import ScanMetadata from "./components/ScanMetadata";
import Settings from "./components/Settings";
import FormContainer from "./component_library/FormContainer";
import { phosphorIcons } from './assets/icons';

import { useAPXPS } from "./hooks/useAPXPS";
export default function App() {

  const {
    rawArray,
    vfftArray,
    ifftArray,
    singlePeakData,
    allPeakData,
    messages,
    wsUrl,
    setWsUrl,
    frameNumber,
    socketStatus,
    startWebSocket,
    closeWebSocket,
    warningMessage,
    status,
    heatmapSettings,
    handleHeatmapSettingChange,
    metadata,
    shotRecentArray,
    shotNumber,
    shotMeanArray,
    shotStdArray,
  } = useAPXPS({});

  //Automatically start the websocket connection on page load
  useEffect(() => {
    startWebSocket();
    return closeWebSocket;
  }, []);

    return (
      <div className="flex-col h-screen w-screen">

        <div className="h-16 shadow-lg">
          <Header />
        </div>

        <div className="flex h-[calc(100vh-4rem)]">
          <Sidebar>
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
            <SidebarItem title='Scan Metadata' icon={phosphorIcons.fileMd}>
              <ScanMetadata status={status} metadata={metadata}/>
            </SidebarItem>
          </Sidebar>

          <Main >
            <Widget title={`Live Images - Current Shot #${shotNumber}`} width='w-3/5' maxWidth='max-w-[1000px]' defaultHeight='h-full' maxHeight='max-h-[1400px]' expandedWidth='w-full'>
              <div className="w-full h-full overflow-auto flex">
                <PlotlyHeatMap array={rawArray} title='RAW' xAxisTitle='' yAxisTitle='' width='w-1/3' verticalScaleFactor={heatmapSettings.scaleFactor.value} showTicks={heatmapSettings.showTicks.value}/>
                <PlotlyHeatMap array={vfftArray} title='VFFT' xAxisTitle='' yAxisTitle='' width='w-1/3' verticalScaleFactor={heatmapSettings.scaleFactor.value} showTicks={heatmapSettings.showTicks.value}/>
                <PlotlyHeatMap array={ifftArray} title='IFFT' xAxisTitle='' yAxisTitle='' width='w-1/3' verticalScaleFactor={heatmapSettings.scaleFactor.value} showTicks={heatmapSettings.showTicks.value}/>
              </div>
            </Widget>

            <div className='flex flex-wrap w-2/5 h-full'>
              <Widget title={`Shot Sum - Current Shot #${shotNumber}`} width='w-full' maxWidth='max-w-[1000px]' defaultHeight='h-1/2' maxHeight='max-h-[1000px]' contentStyles='flex-col space-y-1 pb-2'>
                <PlotlyHeatMap array={shotRecentArray} title='Shot Recent' fixPlotHeightToParent={true} height="h-1/3" width='w-full' verticalScaleFactor={1} showTicks={false}/>
                <PlotlyHeatMap array={shotMeanArray} title='Shot Mean' fixPlotHeightToParent={true} height="h-1/3" width='w-full' verticalScaleFactor={1} showTicks={false}/>
                <PlotlyHeatMap array={shotStdArray} title='Shot Std' fixPlotHeightToParent={true} height="h-1/3" width='w-full' verticalScaleFactor={1} showTicks={false}/>
              </Widget>
              <Widget title='Recent Fitted Peaks' width='w-full' maxWidth='max-w-[1000px]' defaultHeight='h-1/4' maxHeight='max-h-96'>
                  <PlotlyScatterMultiple data={singlePeakData} title='Recent Fitted Peaks' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
              <Widget title='Cumulative Fitted Peaks' width='w-full' maxWidth='max-w-[1000px]' defaultHeight='h-1/4' maxHeight='max-h-96'>
                  <PlotlyScatterMultiple data={allPeakData} title='Cumulative Fitted Peaks' xAxisTitle='x' yAxisTitle='y'/>
              </Widget>
            </div>
          </Main>
        </div>
      </div>

    )
}
