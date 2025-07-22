import { Routes, Route } from 'react-router';
import Home from './pages/Home';
import HomeAlternate from './pages/HomeAlternate';
import Services from './pages/Services';
import TimelineTiledHeatmapScatter from './components/TimelineTiledHeatmapScatter';
export default function App() {
    return (
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="*" element={<Home />} />
        <Route path="/services" element={<Services />} />
        <Route path="/alternate" element={<HomeAlternate />} />
        <Route path="/test" element={
          <div className='w-[1000px] h-[1000px]'>
              <TimelineTiledHeatmapScatter tiledLinks={[{curve:"http://127.0.0.1:8000/api/v1/table/partition/short_table?partition=0&format=application/json-seq"}]}/>
          </div>
      
      } />
      </Routes>
    )
}
