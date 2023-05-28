// State whether price and volume increased or decreased
export function processChartDataChange(data) {
  let previousPrice = data[0].price;
  let previousVolume = data[0].volume;

  return data.map((point, index) => {
    if (index === 0) {
      return { ...point, volumeIncreased: true, priceIncreased: true };
    }

    let volumeIncreased = point.volume > previousVolume;
    let priceIncreased = point.price > previousPrice;
    previousVolume = point.volume;
    previousPrice = point.price;

    return { ...point, volumeIncreased, priceIncreased };
  });
}

export function formatDateLocalString(date) {
  try {
    return new Date(date).toLocaleString();
  } catch (error) {
    return "Invalid Date";
  }
}

export function processDatumToStr(datum) {
  const str_date = new Date(datum.x).toLocaleDateString();
  const str_open = datum.open.toFixed(2);
  const str_high = datum.high.toFixed(2);
  const str_low = datum.low.toFixed(2);
  const str_close = datum.close.toFixed(2);
  const str_volume = datum.volume.toFixed(2);
  return `${str_date} O: ${str_open} H: ${str_high} L: ${str_low} C: ${str_close} V:${str_volume}`;
}
