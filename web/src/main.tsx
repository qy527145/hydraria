import React from 'react';
import ReactDOM from 'react-dom/client';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import App from './App';
import './app.css';

/** 深色主题令牌与 app.css 里的 CSS 变量保持一致，避免两套颜色打架。 */
const hydrariaTheme = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: '#6ea8ff',
    colorInfo: '#38bdf8',
    colorSuccess: '#4ade80',
    colorWarning: '#fbbf24',
    colorError: '#f87171',
    colorBgBase: '#0a0c12',
    colorBgContainer: '#141823',
    colorBorder: '#262c3a',
    borderRadius: 10,
    fontFamily: '-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif',
  },
};

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider theme={hydrariaTheme} locale={zhCN}>
      <App />
    </ConfigProvider>
  </React.StrictMode>,
);
