const apartmentSelect = document.querySelector("#apartmentSelect");
const latestPrice = document.querySelector("#latestPrice");
const changePct = document.querySelector("#changePct");
const pointCount = document.querySelector("#pointCount");
const priceRows = document.querySelector("#priceRows");
const chartCanvas = document.querySelector("#priceChart");
const priceForm = document.querySelector("#priceForm");
const apartmentName = document.querySelector("#apartmentName");
const observedDate = document.querySelector("#observedDate");
const entryMessage = document.querySelector("#entryMessage");

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

function localDateString(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
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

function selectApartment(name) {
  let option = [...apartmentSelect.options].find((item) => item.value === name);
  if (!option) {
    option = new Option(name, name);
    apartmentSelect.add(option);
  }
  apartmentSelect.value = name;
}

async function savePrice(event) {
  event.preventDefault();
  entryMessage.textContent = "저장 중...";
  entryMessage.dataset.state = "";

  const payload = Object.fromEntries(new FormData(priceForm).entries());
  const response = await fetch("/api/prices", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();

  if (!response.ok) {
    entryMessage.textContent = data.error || "저장하지 못했습니다.";
    entryMessage.dataset.state = "error";
    return;
  }

  selectApartment(data.apartment_name);
  priceForm.reset();
  apartmentName.value = data.apartment_name;
  observedDate.value = localDateString();
  entryMessage.textContent = "저장했습니다.";
  entryMessage.dataset.state = "success";
  await loadPrices();
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

  const rows = points.slice().reverse().map((point) => {
    const row = document.createElement("tr");
    const values = [
      point.observed_date,
      point.source,
      point.area_m2 ? `${point.area_m2}㎡` : "-",
      formatKrw(point.price_krw),
      point.note || ""
    ];
    for (const value of values) {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.append(cell);
    }
    return row;
  });
  priceRows.replaceChildren(...rows);

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
priceForm.addEventListener("submit", savePrice);
apartmentName.value = apartmentSelect.value;
observedDate.value = localDateString();
loadPrices();
