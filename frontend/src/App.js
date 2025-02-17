import { Routes, Route } from 'react-router';
import Home from './pages/Home';
import Services from './pages/Services';

export default function App() {
    return (
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="services" element={<Services />} />
      </Routes>
    )
}
