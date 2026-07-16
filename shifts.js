const SHIFT_RULES = {
  "5-2": {
    label: "Shift 5-2",
    workStart: "17:00",
    workEnd: "02:00",
    validCheckInFrom: "13:00",
    validCheckInTo: "19:00",
    validCheckOutFrom: "01:50",
    validCheckOutTo: "10:00",
    checkOutDayOffset: 1,
    lateAllowableMin: 10,
  },
  "6-3": {
    label: "Shift 6-3",
    workStart: "18:00",
    workEnd: "03:00",
    validCheckInFrom: "14:00",
    validCheckInTo: "19:00",
    validCheckOutFrom: "02:50",
    validCheckOutTo: "11:00",
    checkOutDayOffset: 1,
    lateAllowableMin: 10,
  },
  "7-4": {
    label: "Shift 7-4",
    workStart: "19:00",
    workEnd: "04:00",
    validCheckInFrom: "15:00",
    validCheckInTo: "20:00",
    validCheckOutFrom: "03:50",
    validCheckOutTo: "12:00",
    checkOutDayOffset: 1,
    lateAllowableMin: 10,
  },
};

module.exports = SHIFT_RULES;
