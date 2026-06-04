const apartmentSelect = document.querySelector("#apartmentSelect");
const latestPrice = document.querySelector("#latestPrice");
const changePct = document.querySelector("#changePct");
const pointCount = document.querySelector("#pointCount");
const priceRows = document.querySelector("#priceRows");
const chartCanvas = document.querySelector("#priceChart");

let chart;

function formatKrw(value) {
  if (!Number.isFinite(value)) return "-";
  const eok = value / 100000000;
  return `${eok.toLocaleString("ko-KR", { maximumFractionDigits: 2 })}억`;
}

function formatPct(value) {
  if (!Number.isFinite(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function groupByDate(points) {
  const grouped = new Map();
  for (const point of points) {
    const current = grouped.get(point.observed_date) || [];
    current.push(point);
    grouped.set(point.observed_date, current);
  }
  return [...grouped.entries()].map(([date, rows]) => {
    const avg = rows.reduce((sum, row) => sum + row.price_krw, 0) / rows.length;
    return { date, price: Math.round(avg) };
  });
}

async function loadPrices() {
  const apartment = apartmentSelect.value;
  const response = await fetch(`/api/prices?apartment=${encodeURIComponent(apartment)}`);
  const data = await response.json();
  render(data.points);
}

function render(points) {
  const series = groupByDate(points);
  const first = series[0]?.price;
  const last = series[series.length - 1]?.price;
  const change = first ? ((last / first) - 1) * 100 : NaN;

  latestPrice.textContent = formatKrw(last);
  changePct.textContent = formatPct(change);
  changePct.dataset.direction = change >= 0 ? "up" : "down";
  pointCount.textContent = points.length.toLocaleString("ko-KR");

  priceRows.innerHTML = points.slice().reverse().map((point) => `
    <tr>
      <td>${point.observed_date}</td>
      <td>${point.source}</td>
      <td>${point.area_m2 ? `${point.area_m2}㎡` : "-"}</td>
      <td>${formatKrw(point.price_krw)}</td>
      <td>${point.note || ""}</td>
    </tr>
  `).join("");

  const chartData = {
    labels: series.map((point) => point.date),
    datasets: [{
      label: "평균 가격",
      data: series.map((point) => point.price),
      borderColor: "#1f7a5f",
      backgroundColor: "rgba(31, 122, 95, 0.12)",
      fill: true,
      pointRadius: 4,
      tension: 0.25
    }]
  };

  if (chart) {
    chart.data = chartData;
    chart.update();
    return;
  }

  chart = new Chart(chartCanvas, {
    type: "line",
    data: chartData,
    options: {
      responsive: true,
      maintainAspectRatio: true,
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => formatKrw(context.parsed.y)
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: (value) => formatKrw(value)
          }
        }
      }
    }
  });
}

apartmentSelect.addEventListener("change", loadPrices);
loadPrices();
