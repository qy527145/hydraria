interface Props {
  /** 吞吐采样，每秒一个点，最旧在前。 */
  samples: number[];
  width?: number;
  height?: number;
  color?: string;
}

/**
 * 实时吞吐折线。刻意不用图表库：卡片上每秒重绘一次，一条 polyline 就够，
 * 也不会为了 20 个点拖进几十 KB 的依赖。
 *
 * 纵轴按自身峰值归一化——这里要看的是"有没有在动、抖不抖"，绝对值旁边的
 * 数字已经写着了。
 */
export default function Sparkline({ samples, width = 104, height = 22, color = '#6ea8ff' }: Props) {
  const peak = Math.max(...samples, 0);
  // 全零时不画。一条贴底的水平线跟卡片上的分隔线长得一样，反而像是渲染出了 bug。
  if (samples.length < 2 || peak === 0) return <svg width={width} height={height} aria-hidden />;
  const step = width / (samples.length - 1);
  const points = samples
    .map((value, index) => {
      const y = height - (value / peak) * (height - 2) - 1;
      return `${(index * step).toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-label="实时吞吐">
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
