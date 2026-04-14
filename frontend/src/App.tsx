import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Catalog from './views/Catalog'
import Lineage from './views/Lineage'
import Governance from './views/Governance'
import Assets from './views/Assets'
import Risk from './views/Risk'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Catalog />} />
        <Route path="lineage/:assetId" element={<Lineage />} />
        <Route path="lineage" element={<Lineage />} />
        <Route path="governance" element={<Governance />} />
        <Route path="assets" element={<Assets />} />
        <Route path="risk" element={<Risk />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
