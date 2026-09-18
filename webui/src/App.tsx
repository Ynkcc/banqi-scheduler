import { Layout, Menu } from 'antd'
import { Link, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import Episodes from './pages/Episodes'
import Eval from './pages/Eval'
import Matches from './pages/Matches'
import Networks from './pages/Networks'
import Overview from './pages/Overview'
import TrainConfig from './pages/TrainConfig'
import Workers from './pages/Workers'

const menuItems = [
  { key: '/', label: <Link to="/">总览</Link> },
  { key: '/networks', label: <Link to="/networks">网络</Link> },
  { key: '/matches', label: <Link to="/matches">对战 / SPRT</Link> },
  { key: '/eval', label: <Link to="/eval">绝对强度</Link> },
  { key: '/train-config', label: <Link to="/train-config">训练超参</Link> },
  { key: '/workers', label: <Link to="/workers">Worker</Link> },
  { key: '/episodes', label: <Link to="/episodes">Episode</Link> },
]

export default function App() {
  const { pathname } = useLocation()
  const selected = menuItems.some((item) => item.key === pathname) ? pathname : '/'

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="dark" width={208}>
        <div className="brand">banqi scheduler</div>
        <Menu theme="dark" mode="inline" selectedKeys={[selected]} items={menuItems} />
      </Layout.Sider>
      <Layout>
        <Layout.Content style={{ padding: 24 }}>
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/networks" element={<Networks />} />
            <Route path="/matches" element={<Matches />} />
            <Route path="/eval" element={<Eval />} />
            <Route path="/train-config" element={<TrainConfig />} />
            <Route path="/workers" element={<Workers />} />
            <Route path="/episodes" element={<Episodes />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Layout.Content>
      </Layout>
    </Layout>
  )
}
