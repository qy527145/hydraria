import { Tooltip } from 'antd';
import { formatBytes } from '../../utils/format';

interface Props {
  /** 每格一个区间的落盘百分比（0-100），由后端位图聚合而来。 */
  values: number[];
  /** 文件总大小，用于把格子索引换算回字节区间。 */
  total: number;
}

/** 缓存分布热力条：越亮表示该区间落盘越多，空条表示还没有任何数据。 */
export default function CacheHeatmap({ values, total }: Props) {
  if (!values.length) return <div className="cache-heat empty" aria-label="暂无缓存数据" />;
  return (
    <div className="cache-heat" aria-label="缓存分布">
      {values.map((value, index) => {
        const lo = Math.floor((index * total) / values.length);
        const hi = Math.floor(((index + 1) * total) / values.length) - 1;
        return (
          <Tooltip key={index} title={`${value}% · ${formatBytes(lo)} – ${formatBytes(hi)}`}>
            <span style={{ background: `rgba(56,189,248,${0.06 + (value / 100) * 0.94})` }} />
          </Tooltip>
        );
      })}
    </div>
  );
}
