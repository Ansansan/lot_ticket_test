const tg = window.Telegram.WebApp;
tg.expand();
const ASSET_BASE = new URL('.', window.location.href).href;

// --- CONFIGURATION ---
const API_URL = "https://tel.pythonanywhere.com";

const STANDARD_LOTTERIES = [
    { id: "primera_11", name: "La Primera", time: "11:00 am", icon: "🇩🇴" },
    { id: "nica_1", name: "Nica", time: "1:00 pm", icon: "🇳🇮" },
    { id: "tica_1", name: "Tica", time: "1:55 pm", icon: "🇨🇷" },
    { id: "nica_4", name: "Nica", time: "4:00 pm", icon: "🇳🇮" },
    { id: "tica_5", name: "Tica", time: "5:30 pm", icon: "🇨🇷" },
    { id: "primera_6", name: "La Primera", time: "6:00 pm", icon: "🇩🇴" },
    { id: "nica_7", name: "Nica", time: "7:00 pm", icon: "🇳🇮" },
    { id: "tica_8", name: "Tica", time: "8:30 pm", icon: "🇨🇷" },
    { id: "nica_10", name: "Nica", time: "10:00 pm", icon: "🇳🇮" }
];
const NACIONAL_LOTTERY = { id: "nacional", name: "Nacional", time: "3:00 pm", icon: "🇵🇦", special: true };
const AWARDS = {
    '2_digit_1': 14.00, '2_digit_2': 3.00, '2_digit_3': 2.00,
    '4_digit_12': 1000.00, '4_digit_13': 1000.00, '4_digit_23': 200.00
};

let currentState = {
    mode: 'user', date: null, displayDate: null, lottery: null, items: [],
    activeNacionalDates: [], history: { tickets: [], results: {} },
    historyDate: null, historyLottery: null, statsDate: null, walletBalance: 0,
    editingTicketId: null, editingRowIndex: null, receiptManual: null, draftId: null,
    lockedDraftContext: null
};

window.onload = function () {
    const urlParams = new URLSearchParams(window.location.search);
    const mode = urlParams.get('mode');
    const datesParam = urlParams.get('nacional_dates');
    const balanceParam = urlParams.get('bal');

    if (datesParam) {
        currentState.activeNacionalDates = datesParam.split(',').map(d => d.trim()).filter(Boolean);
    }
    if (balanceParam !== null) {
        const parsedBalance = parseFloat(balanceParam);
        if (!Number.isNaN(parsedBalance)) currentState.walletBalance = parsedBalance;
    }

    const panamaNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Panama" }));
    const pYear = panamaNow.getFullYear();
    const pMonth = String(panamaNow.getMonth() + 1).padStart(2, '0');
    const pDay = String(panamaNow.getDate()).padStart(2, '0');
    const todayStr = `${pYear}-${pMonth}-${pDay}`;

    currentState.date = todayStr;
    const adminDate = document.getElementById('adminDate');
    if (adminDate) adminDate.value = todayStr;

    renderDateScroller(panamaNow);
    renderLotteryGridForDate(todayStr);
    setupInputListeners();
    setupAdminDashboardListeners();
    renderAvailableFunds();

    // 🟢 ROUTING
    if (mode === 'admin_dashboard') {
        currentState.mode = 'admin';
        showPage('page-admin-dashboard');
    }
    else if (mode === 'admin') {
        currentState.mode = 'admin';
        showPage('page-admin');
        populateAdminSelect();
    }
    else if (mode === 'history') {
        currentState.mode = 'history';
        showPage('page-history');

        let attempts = 0;
        const maxAttempts = 20;

        function tryLoadData() {
            // 🛑 FIX: Use explicit FORCE_ID_ routing
            if (tg.initDataUnsafe && tg.initDataUnsafe.user) {
                console.log("Authenticated via Telegram User ID");
                const forcedAuth = "FORCE_ID_" + tg.initDataUnsafe.user.id;
                loadHistoryData(forcedAuth, panamaNow);
            }
            // Fallback for debugging via URL (Legacy)
            else {
                const urlParams = new URLSearchParams(window.location.search);
                const forcedUid = urlParams.get('uid');

                if (forcedUid) {
                    console.log("Using URL ID:", forcedUid);
                    loadHistoryData("FORCE_ID_" + forcedUid, panamaNow);
                }
                else if (attempts < maxAttempts) {
                    attempts++;
                    const statusEl = document.getElementById('historyStatus');
                    if (statusEl) {
                        statusEl.innerText = `Buscando ID... (${attempts})`;
                        statusEl.style.display = 'block';
                    }
                    setTimeout(tryLoadData, 200);
                }
                else {
                    setHistoryStatus("Error: Identidad no encontrada.");
                    alert("⚠️ Error: No se detectó tu usuario.\nPor favor escribe /start de nuevo.");
                }
            }
        }

        tg.ready();
        tryLoadData();

    } else if (mode === 'receipt_manual') {
        currentState.mode = 'receipt_manual';
        initReceiptManualPage(urlParams);
        showPage('page-receipt-manual');

    } else if (mode === 'draft_edit') {
        currentState.mode = 'user';
        loadDraftEditFromUrl(urlParams);

    } else {
        showPage('page-menu');
    }
};

// --- API LOADER ---
function loadHistoryData(telegramData, panamaNow) {
    setHistoryStatus("Entrando...");

    if (!telegramData) {
        alert("⛔ Error Crítico: Telegram Data Vacío.");
        setHistoryStatus("Error: No Identidad");
        initHistoryView(panamaNow);
        return;
    }

    fetch(`${API_URL}/history`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData: telegramData })
    })
        .then(res => {
            if (res.status === 401) {
                return res.json().then(errData => {
                    alert("🚨 ERROR REAL DEL SERVIDOR:\n" + errData.error);
                    setHistoryStatus("Error: " + errData.error);
                });
            }
            if (!res.ok) {
                setHistoryStatus("Error Servidor: " + res.status);
                return null;
            }
            return res.json();
        })
        .then(data => {
            if (data && data.ok) {
                currentState.history = data.data;
                setHistoryStatus("");
            } else if (data) {
                setHistoryStatus("No tienes tickets jugados.");
            }
            initHistoryView(panamaNow);

            // Auto-open ticket for editing if edit_ticket param is present
            const editTicketId = new URLSearchParams(window.location.search).get('edit_ticket');
            if (editTicketId && currentState.history.tickets) {
                const ticket = currentState.history.tickets.find(t => t.id === parseInt(editTicketId));
                if (ticket) editTicket(ticket.id, ticket.lottery_type, ticket.date);
            }
        })
        .catch(err => {
            setHistoryStatus("Error de conexión");
            initHistoryView(panamaNow);
        });
}

function renderDateScroller(startDate) {
    const container = document.getElementById('customDateScroller');
    container.innerHTML = "";
    for (let i = 0; i < 2; i++) {
        const d = new Date(startDate);
        d.setDate(d.getDate() + i);
        const year = d.getFullYear(); const month = String(d.getMonth() + 1).padStart(2, '0'); const day = String(d.getDate()).padStart(2, '0');
        const isoDate = `${year}-${month}-${day}`;
        const days = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'];
        const isToday = i === 0;
        let label = isToday ? "HOY" : "MAÑANA";
        if (!isToday) label = `${days[d.getDay()]} ${d.getDate()}`;

        const chip = document.createElement('div');
        chip.className = `date-chip ${isToday ? 'selected' : ''}`;
        chip.innerText = label;
        chip.onclick = () => selectDate(chip, isoDate, label);
        container.appendChild(chip);
        if (isToday) currentState.displayDate = label;
    }
}

function renderCard(lot, container, isHighlight) {
    const card = document.createElement('div');
    card.className = "lottery-card";
    if (lot.special) card.classList.add('card-nacional');
    if (isHighlight && !lot.special) {
        card.style.border = "2px solid #3390ec"; card.style.background = "#f0f8ff";
    }
    const iconHtml = buildIconHtml(lot.icon);
    card.innerHTML = `${iconHtml}<div class="card-name">${lot.name}</div><div class="card-time">${lot.time}</div>`;
    card.onclick = () => selectLottery(lot);
    container.appendChild(card);
}

function getMinutesFromTime(timeStr) {
    const [time, modifier] = timeStr.split(' ');
    let [hours, minutes] = time.split(':');
    hours = parseInt(hours); minutes = parseInt(minutes);
    if (hours === 12 && modifier.toLowerCase() === 'am') hours = 0;
    if (hours !== 12 && modifier.toLowerCase() === 'pm') hours += 12;
    return (hours * 60) + minutes;
}

function selectDate(element, dateStr, label) {
    currentState.date = dateStr;
    currentState.displayDate = label;
    document.querySelectorAll('.date-chip').forEach(c => c.classList.remove('selected'));
    element.classList.add('selected');
    renderLotteryGridForDate(dateStr);
}

function renderLotteryGridForDate(dateStr) {
    const grid = document.getElementById('lotteryGrid');
    grid.innerHTML = "";

    const panamaNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Panama" }));
    const pYear = panamaNow.getFullYear(); const pMonth = String(panamaNow.getMonth() + 1).padStart(2, '0'); const pDay = String(panamaNow.getDate()).padStart(2, '0');
    const panamaDateStr = `${pYear}-${pMonth}-${pDay}`;
    const isTodayView = (dateStr === panamaDateStr);

    let availableDraws = [];
    if (isTodayView) {
        let allLotteries = [...STANDARD_LOTTERIES];
        if (currentState.activeNacionalDates.includes(dateStr)) {
            allLotteries.splice(3, 0, NACIONAL_LOTTERY);
        }
        const currentMinutes = (panamaNow.getHours() * 60) + panamaNow.getMinutes();
        availableDraws = allLotteries.filter(lot => {
            const drawMinutes = getMinutesFromTime(lot.time);
            if (lot.id === 'nacional') return currentMinutes < 901;
            return currentMinutes < drawMinutes;
        });
    } else {
        availableDraws = STANDARD_LOTTERIES.filter(lot => lot.id === 'primera_11' || lot.id === 'nica_1');
        if (currentState.activeNacionalDates.includes(dateStr)) {
            availableDraws = [NACIONAL_LOTTERY, ...availableDraws];
        }
    }

    if (availableDraws.length === 0) {
        grid.innerHTML = "<div style='grid-column: span 2; text-align: center; color: #888; padding: 20px;'>No hay sorteos disponibles.</div>";
        return;
    }

    if (isTodayView) {
        const nacional = availableDraws.find(l => l.id === 'nacional');
        const standardDraws = availableDraws.filter(l => l.id !== 'nacional');
        const titleActual = document.createElement('div');
        titleActual.className = 'section-title';
        titleActual.innerHTML = "⚡ SORTEO ACTUAL";
        titleActual.style.cssText = "grid-column: span 2; color: #3390ec; font-weight: bold; margin-top: 10px;";
        grid.appendChild(titleActual);
        if (nacional) renderCard(nacional, grid, true);
        if (standardDraws.length > 0) {
            renderCard(standardDraws[0], grid, true);
            const others = standardDraws.slice(1);
            if (others.length > 0) {
                const titleOthers = document.createElement('div');
                titleOthers.className = 'section-title';
                titleOthers.innerText = "OTROS";
                titleOthers.style.cssText = "grid-column: span 2; color: #666; font-weight: bold; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;";
                grid.appendChild(titleOthers);
                others.forEach(lot => renderCard(lot, grid, false));
            }
        }
    } else {
        availableDraws.forEach(lot => renderCard(lot, grid, false));
    }
}

function selectLottery(lotteryObj) {
    currentState.lottery = lotteryObj.name + " " + lotteryObj.time;
    currentState.lockedDraftContext = null;
    syncInputHeader();
    showPage('page-input');
}

function showPage(pageId) {
    if (pageId === 'page-menu' && currentState.lockedDraftContext) {
        pageId = 'page-input';
    }
    document.querySelectorAll('.page').forEach(p => p.classList.add('hidden'));
    document.getElementById(pageId).classList.remove('hidden');
    if (pageId === 'page-input' && currentState.items.length > 0) {
        tg.MainButton.show();
    } else {
        tg.MainButton.hide();
    }
}

function syncInputHeader() {
    const display = document.getElementById('selectedDrawDisplay');
    const backBtn = document.querySelector('#page-input .back-btn');
    if (!display || !backBtn) return;

    if (currentState.editingTicketId) {
        display.innerText = `Editando Ticket #${currentState.editingTicketId}`;
        backBtn.innerText = '⬅️ Atrás';
        backBtn.onclick = () => goBack();
        return;
    }

    if (currentState.lockedDraftContext) {
        const lockedLottery = currentState.lockedDraftContext.lottery || currentState.lottery || 'Sorteo';
        const lockedDate = currentState.lockedDraftContext.date || currentState.date || '';
        display.innerText = lockedDate ? `${lockedLottery} (${lockedDate})` : lockedLottery;
        backBtn.innerText = 'Cerrar';
        backBtn.onclick = () => closeLockedDraftEditor();
        return;
    }

    const dateLabel = currentState.displayDate || currentState.date || '';
    display.innerText = dateLabel ? `${currentState.lottery} (${dateLabel})` : (currentState.lottery || 'Sorteo');
    backBtn.innerText = '⬅️ Atrás';
    backBtn.onclick = () => goBack();
}

function closeLockedDraftEditor() {
    if (tg && typeof tg.close === 'function') {
        tg.close();
        return;
    }
    currentState.draftId = null;
    currentState.lockedDraftContext = null;
    showPage('page-menu');
}

function loadDraftEditFromUrl(urlParams) {
    const draftIdRaw = urlParams.get('draft_id');
    const draftLottery = (urlParams.get('draft_lottery') || '').trim();
    const draftDate = (urlParams.get('draft_date') || currentState.date || '').trim();
    const draftItemsRaw = urlParams.get('draft_items') || '[]';
    let draftItems = [];

    try {
        draftItems = JSON.parse(draftItemsRaw);
    } catch (error) {
        showTelegramAlert("No pude abrir este borrador.");
        showPage('page-menu');
        return;
    }

    currentState.draftId = draftIdRaw ? parseInt(draftIdRaw, 10) : null;
    currentState.editingTicketId = null;
    currentState.editingRowIndex = null;
    currentState.mode = 'draft_edit';
    currentState.date = draftDate || currentState.date;
    currentState.displayDate = currentState.date;
    currentState.lottery = draftLottery;
    currentState.lockedDraftContext = { date: draftDate, lottery: draftLottery };
    currentState.items = (draftItems || []).map(item => {
        if (item && item.separator) {
            return { num: '---', qty: 0, totalLine: 0, separator: true };
        }
        const num = String((item && item.num) || '').trim();
        const qty = parseInt(item && item.qty, 10) || 0;
        const priceUnit = num.length === 4 ? 1.0 : 0.25;
        return { num, qty, totalLine: priceUnit * qty };
    }).filter(item => item.separator || (item.num && item.qty > 0));

    if (!currentState.lottery || currentState.items.length === 0) {
        showTelegramAlert("Este borrador está incompleto.");
        showPage('page-menu');
        return;
    }

    syncInputHeader();
    renderList();
    showPage('page-input');
}

window.goBack = function () {
    if (currentState.lockedDraftContext) {
        closeLockedDraftEditor();
        return;
    }
    if (currentState.editingTicketId) {
        currentState.editingTicketId = null;
        currentState.editingRowIndex = null;
        currentState.mode = 'history';
        currentState.items = [];
        renderList();
        showPage('page-history');
    } else {
        currentState.draftId = null;
        currentState.lockedDraftContext = null;
        currentState.editingRowIndex = null;
        currentState.items = [];
        renderList();
        showPage('page-menu');
    }
};

function showTelegramAlert(message) {
    if (tg && typeof tg.showAlert === 'function') {
        tg.showAlert(message);
    } else {
        alert(message);
    }
}

function normalizeManualTimeInput(rawValue) {
    const cleaned = String(rawValue || '').trim();
    if (!cleaned) return "";
    const match = cleaned.match(/(\d{1,2}):(\d{2})/);
    if (!match) return null;
    return `${parseInt(match[1], 10)}:${match[2]}`;
}

window.initReceiptManualPage = function (urlParams) {
    currentState.receiptManual = {
        followupId: (urlParams.get('followup_id') || '').trim(),
        submitted: false
    };

    const amountInput = document.getElementById('manualReceiptAmount');
    const confirmationInput = document.getElementById('manualReceiptConfirmation');
    const timeInput = document.getElementById('manualReceiptTime');

    if (amountInput) amountInput.value = (urlParams.get('amount') || '').trim();
    if (confirmationInput) confirmationInput.value = (urlParams.get('confirmation') || '').trim();
    if (timeInput) timeInput.value = (urlParams.get('receipt_time') || '').trim();

    if (amountInput) {
        setTimeout(() => amountInput.focus(), 150);
    }
};

window.submitManualReceipt = function () {
    const followupId = currentState.receiptManual && currentState.receiptManual.followupId;
    const amountInput = document.getElementById('manualReceiptAmount');
    const confirmationInput = document.getElementById('manualReceiptConfirmation');
    const timeInput = document.getElementById('manualReceiptTime');
    const amount = ((amountInput && amountInput.value) || '').trim();
    const confirmation = ((confirmationInput && confirmationInput.value) || '').trim();
    const receiptTimeRaw = ((timeInput && timeInput.value) || '').trim();
    const receiptTime = normalizeManualTimeInput(receiptTimeRaw);

    if (!followupId) {
        showTelegramAlert("No pude identificar este comprobante.");
        return;
    }
    if (!amount) {
        showTelegramAlert("Falta el monto.");
        return;
    }
    if (!confirmation) {
        showTelegramAlert("Falta el código de confirmación.");
        return;
    }
    if (receiptTime === null) {
        showTelegramAlert("La hora debe verse como 1:06.");
        return;
    }
    // Inline WebApp launches are valid again for manual receipt corrections.
    if (false && tg && tg.initDataUnsafe && tg.initDataUnsafe.query_id) {
        showTelegramAlert("Este formulario ya no envía datos desde ese botón. Cierra esta ventana y responde en el chat con monto, confirmación y hora.");
        return;
    }

    tg.sendData(JSON.stringify({
        action: 'manual_receipt_submit',
        followup_id: followupId,
        amount: amount,
        confirmation: confirmation,
        receipt_time: receiptTime
    }));
    if (currentState.receiptManual) {
        currentState.receiptManual.submitted = true;
    }
    if (tg && typeof tg.close === 'function') {
        setTimeout(() => tg.close(), 50);
    }
};

window.closeManualReceiptApp = function () {
    const followupId = currentState.receiptManual && currentState.receiptManual.followupId;
    const submitted = currentState.receiptManual && currentState.receiptManual.submitted;
    if (!submitted && followupId && tg && typeof tg.sendData === 'function') {
        tg.sendData(JSON.stringify({
            action: 'manual_receipt_closed',
            followup_id: followupId
        }));
    }
    if (tg && typeof tg.close === 'function') {
        tg.close();
    }
};

// 🟢 2-BOX INPUT LOGIC
function setupInputListeners() {
    const numInput = document.getElementById('numInput');
    const qtyInput = document.getElementById('qtyInput');
    const formatError = document.getElementById('formatError');
    numInput.addEventListener('input', function () {
        const val = this.value;
        if (val.length > 0 && val.length !== 2 && val.length !== 4) {
            formatError.style.display = 'block'; numInput.style.borderColor = '#ff3b30';
        } else {
            formatError.style.display = 'none'; numInput.style.borderColor = '#ccc';
        }
    });
    numInput.addEventListener("keydown", function (event) {
        if (event.key === "Enter") { event.preventDefault(); qtyInput.focus(); }
    });
    qtyInput.addEventListener("keydown", function (event) {
        if (event.key === "Enter") { event.preventDefault(); addItem(); }
    });

    const decenaInput = document.getElementById('decenaInput');
    const decenaQtyInput = document.getElementById('decenaQtyInput');
    if (decenaInput) {
        decenaInput.addEventListener('input', function () {
            this.value = this.value.replace(/\D/g, '').slice(0, 2);
        });
        decenaInput.addEventListener("keydown", function (event) {
            if (event.key === "Enter") {
                event.preventDefault();
                if (decenaQtyInput) decenaQtyInput.focus();
            }
        });
    }
    if (decenaQtyInput) {
        decenaQtyInput.addEventListener('input', function () {
            this.value = this.value.replace(/\D/g, '').slice(0, 4);
        });
        decenaQtyInput.addEventListener("keydown", function (event) {
            if (event.key === "Enter") {
                event.preventDefault();
                confirmDecenaModal();
            }
        });
    }
}

window.addItem = function () {
    const numInput = document.getElementById('numInput');
    const qtyInput = document.getElementById('qtyInput');
    const errorMsg = document.getElementById('errorMsg');
    const formatError = document.getElementById('formatError');
    const num = numInput.value.trim();
    const qtyVal = qtyInput.value.trim();
    const qty = qtyVal === "" ? 1 : parseInt(qtyVal);
    if (!num) { showError("Ingresa un número"); return; }
    if (qty < 1) { showError("Cantidad inválida"); return; }
    let priceUnit = 0;
    if (num.length === 2) priceUnit = 0.25; else if (num.length === 4) priceUnit = 1.00; else { showError("Solo 2 o 4 dígitos"); return; }
    const totalLine = priceUnit * qty;
    currentState.items.push({ num, qty, totalLine });
    renderList();
    numInput.value = ""; qtyInput.value = ""; errorMsg.innerText = "";
    formatError.style.display = 'none'; numInput.style.borderColor = '#ccc'; numInput.focus();
};

function renderAvailableFunds() {
    const fundsEl = document.getElementById('availableFunds');
    if (!fundsEl) return;
    const safeBalance = Number(currentState.walletBalance) || 0;
    fundsEl.innerText = `Fondo disponible: $${safeBalance.toFixed(2)}`;
}

window.openDecenaModal = function () {
    const modal = document.getElementById('decenaModal');
    const decenaInput = document.getElementById('decenaInput');
    const decenaQtyInput = document.getElementById('decenaQtyInput');
    const numInput = document.getElementById('numInput');
    const qtyInput = document.getElementById('qtyInput');

    if (!modal || !decenaInput || !decenaQtyInput) return;

    const seededNum = ((numInput && numInput.value) || "").replace(/\D/g, "");
    const seededQty = ((qtyInput && qtyInput.value) || "").trim();

    decenaInput.value = (seededNum.length === 1 || seededNum.length === 2) ? seededNum : "";
    decenaQtyInput.value = seededQty || "1";

    modal.classList.remove('hidden');
    setTimeout(() => decenaInput.focus(), 50);
};

window.closeDecenaModal = function () {
    const modal = document.getElementById('decenaModal');
    if (modal) modal.classList.add('hidden');
};

window.confirmDecenaModal = function () {
    const decenaInput = document.getElementById('decenaInput');
    const decenaQtyInput = document.getElementById('decenaQtyInput');
    const numInput = document.getElementById('numInput');
    const qtyInput = document.getElementById('qtyInput');
    const errorMsg = document.getElementById('errorMsg');

    if (!decenaInput || !decenaQtyInput) return;

    const raw = (decenaInput.value || "").replace(/\D/g, "");
    const qtyVal = (decenaQtyInput.value || "").trim();
    const qty = qtyVal === "" ? 1 : parseInt(qtyVal, 10);

    if (!raw) { showError("Ingresa la decena (ej. 3 o 30)"); return; }
    if (qty < 1) { showError("Cantidad inválida"); return; }

    let tensDigit;
    if (raw.length === 1) tensDigit = parseInt(raw, 10);
    else if (raw.length === 2) tensDigit = Math.floor(parseInt(raw, 10) / 10);
    else { showError("Para decena usa 1 o 2 dígitos"); return; }

    if (Number.isNaN(tensDigit) || tensDigit < 0 || tensDigit > 9) {
        showError("Decena inválida");
        return;
    }

    for (let i = 0; i < 10; i++) {
        const num = `${tensDigit}${i}`;
        currentState.items.push({ num, qty, totalLine: 0.25 * qty });
    }

    renderList();
    closeDecenaModal();
    errorMsg.innerText = "";
    decenaInput.value = "";
    decenaQtyInput.value = "1";
    if (numInput) numInput.value = "";
    if (qtyInput) qtyInput.value = "";
    if (numInput) numInput.focus();
};

window.addDecena = function () {
    openDecenaModal();
};

// --- PASTE LIST FEATURE ---

let pasteQtyOrder = 'left';
let _pasteSideResolve = null; // for side-chooser promise

window.openPasteModal = function () {
    const modal = document.getElementById('pasteModal');
    const textarea = document.getElementById('pasteTextarea');
    const result = document.getElementById('pasteResult');
    if (!modal || !textarea) return;
    textarea.value = '';
    if (result) result.textContent = '';
    pasteQtyOrder = 'left';
    modal.classList.remove('hidden');
};

window.closePasteModal = function () {
    const modal = document.getElementById('pasteModal');
    if (modal) modal.classList.add('hidden');
};

/**
 * Detect which side is quantity from number patterns.
 * Returns 'left', 'right', or null if uncertain.
 *
 * Signals (applied to ambiguous 2-digit pairs only):
 *  - Single digit  (weight 3): "2" can only be qty (lottery nums are 2 or 4 digits)
 *  - Leading zeros (weight 3): "04" is a lottery number, never a quantity
 *  - Unique count  (weight 2): quantity side repeats (all "1", all "2")
 *  - Lower mean    (weight 1): quantities are typically smaller
 *
 * 4-digit pairs are excluded (already auto-handled by the parser).
 * Explicit text markers ("iz", "der") override everything.
 */
function detectPasteOrder(text) {
    // Explicit markers override
    const hasIz = /\b(iz|izq|izquierda)\b/i.test(text);
    const hasDer = /\b(der|derecha)\b/i.test(text);
    if (hasIz && !hasDer) return 'left';
    if (hasDer && !hasIz) return 'right';

    // Strip WhatsApp prefixes to avoid false pairs from dates like 03/08
    const cleaned = text.replace(/\[[\d/]+,\s*[\d:]+\]\s*[^:\n]+:\s*/g, '');

    // Collect ambiguous pairs (dots, dash, slash, space), skip 4-digit (auto-handled)
    const pairs = [];
    const lines = cleaned.split(/\n/).map(l => l.trim()).filter(Boolean);
    for (const line of lines) {
        const dotMatches = [...line.matchAll(/(\d+)\.{2,}(\d+)/g)];
        if (dotMatches.length > 0) {
            dotMatches.forEach(m => {
                if (m[1].length !== 4 && m[2].length !== 4) pairs.push([m[1], m[2]]);
            });
            continue;
        }
        const dashSlash = [...line.matchAll(/(\d+)\s*[-/]\s*(\d+)/g)];
        if (dashSlash.length > 0) {
            dashSlash.forEach(m => {
                if (m[1].length !== 4 && m[2].length !== 4) pairs.push([m[1], m[2]]);
            });
            continue;
        }
        const spacePair = line.match(/^\s*(\d+)\s+(\d+)\s*$/);
        if (spacePair && spacePair[1].length !== 4 && spacePair[2].length !== 4) {
            pairs.push([spacePair[1], spacePair[2]]);
        }
    }
    if (pairs.length < 2) return null; // not enough data

    // Score: positive → left is qty, negative → right is qty
    let score = 0;
    const lefts = pairs.map(p => p[0]);
    const rights = pairs.map(p => p[1]);

    // Single digit = always quantity (lottery numbers are 2 or 4 digits)
    const leftSingleDigit = lefts.filter(v => v.length === 1).length;
    const rightSingleDigit = rights.filter(v => v.length === 1).length;
    score += (leftSingleDigit - rightSingleDigit) * 3;

    // Leading zeros = lottery number, not quantity
    const leftLeadZeros = lefts.filter(v => v.length > 1 && v[0] === '0').length;
    const rightLeadZeros = rights.filter(v => v.length > 1 && v[0] === '0').length;
    score += (rightLeadZeros - leftLeadZeros) * 3;

    // All multiples of 5 on one side = likely quantity (people buy in round numbers)
    const leftAllMult5 = lefts.every(v => parseInt(v, 10) % 5 === 0);
    const rightAllMult5 = rights.every(v => parseInt(v, 10) % 5 === 0);
    if (leftAllMult5 && !rightAllMult5) score += 2;
    else if (rightAllMult5 && !leftAllMult5) score -= 2;

    // Fewer unique values = likely quantity side
    const leftUnique = new Set(lefts).size;
    const rightUnique = new Set(rights).size;
    if (leftUnique < rightUnique) score += 2;
    else if (rightUnique < leftUnique) score -= 2;

    // Lower mean = likely quantity side
    const leftMean = lefts.reduce((s, v) => s + parseInt(v, 10), 0) / lefts.length;
    const rightMean = rights.reduce((s, v) => s + parseInt(v, 10), 0) / rights.length;
    if (leftMean < rightMean) score += 1;
    else if (rightMean < leftMean) score -= 1;

    if (score >= 2) return 'left';
    if (score <= -2) return 'right';
    return null; // uncertain
}

function askPasteSide() {
    return new Promise(resolve => {
        _pasteSideResolve = resolve;
        document.getElementById('pasteSideModal').classList.remove('hidden');
    });
}

window.resolvePasteSide = function (side) {
    document.getElementById('pasteSideModal').classList.add('hidden');
    if (_pasteSideResolve) { _pasteSideResolve(side); _pasteSideResolve = null; }
};

window.confirmPasteModal = async function () {
    const textarea = document.getElementById('pasteTextarea');
    const result = document.getElementById('pasteResult');
    if (!textarea) return;

    const raw = textarea.value.trim();
    if (!raw) {
        if (result) { result.textContent = 'Pegue una lista primero'; result.style.color = '#ff3b30'; }
        return;
    }

    // Detect or ask
    let order = detectPasteOrder(raw);
    if (!order) order = await askPasteSide();
    if (!order) return; // user chose "Volver"
    pasteQtyOrder = order;

    const parsed = parseTicketList(raw, pasteQtyOrder);
    if (parsed.length === 0) {
        if (result) { result.textContent = 'No se encontraron números válidos'; result.style.color = '#ff3b30'; }
        return;
    }

    parsed.forEach(item => {
        if (item.separator) {
            currentState.items.push({ num: '---', qty: 0, totalLine: 0, separator: true });
        } else {
            const priceUnit = item.num.length === 4 ? 1.00 : 0.25;
            currentState.items.push({ num: item.num, qty: item.qty, totalLine: priceUnit * item.qty });
        }
    });

    renderList();
    closePasteModal();
    const numCount = parsed.filter(p => !p.separator).length;
    showError(`Se agregaron ${numCount} números`);
};

function parseTicketList(rawText, qtyOrder) {
    const results = [];
    // Strip iz/der/izquierda/derecha markers
    let cleaned = rawText.replace(/\b(izquierda|derecha|iz|izq|der)\b/gi, '');
    // Strip WhatsApp multi-select prefixes: [03/08, 16:50] Name:
    cleaned = cleaned.replace(/\[[\d/]+,\s*[\d:]+\]\s*[^:\n]+:\s*/g, '');

    // Split by * or & to create groups separated by divider lines
    const groups = cleaned.split(/[*&]/).map(g => g.trim()).filter(Boolean);
    let stickyQty = 0;

    for (let gi = 0; gi < groups.length; gi++) {
        if (gi > 0) { results.push({ separator: true }); stickyQty = 0; }
        const rawLines = groups[gi].split(/\n/).map(l => l.trim()).filter(Boolean);

        // Further split by " y " separator (e.g. "2 billetes 1946 y 6-46 y 6-80")
        const lines = [];
        for (const rawLine of rawLines) {
            const parts = rawLine.split(/\s+y\s+/i);
            lines.push(...parts.map(p => p.trim()).filter(Boolean));
        }

    for (const line of lines) {
        // Skip empty or marker-only lines
        if (!line || /^\s*$/.test(line)) continue;

        // Sticky qty header: "5 biles cada uno", "10 viles cada", "5 de cada uno"
        const stickyMatch = line.match(/^\s*(\d+)\s*(?:(?:vil(?:es)?|bil(?:es)?|billete(?:s)?|biles?|viles?)\s*(?:de\s+)?|de\s+)cada\s*(?:uno|una)?\s*$/i);
        if (stickyMatch) {
            stickyQty = parseInt(stickyMatch[1], 10);
            continue;
        }

        // Pattern 1: Dots (respects qtyOrder)
        const dotMatches = [...line.matchAll(/(\d+)\.{2,}(\d+)/g)];
        if (dotMatches.length > 0) {
            dotMatches.forEach(m => addParsedPair(results, m[1], m[2], 'dots', qtyOrder));
            continue;
        }

        // Pattern 2: Equals — num = qty
        const eqMatches = [...line.matchAll(/(\d+)\s*=\s*(\d+)/g)];
        if (eqMatches.length > 0) {
            eqMatches.forEach(m => addParsedPair(results, m[2], m[1], 'equals'));
            continue;
        }

        // Pattern 3: Vil/bil/billete keyword + optional "de" — qty vil [de] num
        const vilMatches = [...line.matchAll(/(\d+)\s*(?:vil(?:es)?|bil(?:es)?|billete(?:s)?|biles?|viles?)\s*(?:de\s+)?[-]?\s*(\d+)/gi)];
        if (vilMatches.length > 0) {
            vilMatches.forEach(m => addParsedPair(results, m[1], m[2], 'vil'));
            continue;
        }

        // Pattern 3b: "de" keyword alone — qty de num (e.g. "5 de 11")
        const deMatches = [...line.matchAll(/(\d+)\s+de\s+(\d+)/gi)];
        if (deMatches.length > 0) {
            deMatches.forEach(m => addParsedPair(results, m[1], m[2], 'vil'));
            continue;
        }

        // Pattern 4: Dash/slash pairs (e.g. "20-80 10-08 5-31" or "2/04")
        const dashMatches = [...line.matchAll(/(\d+)\s*[-/]\s*(\d+)/g)];
        if (dashMatches.length > 0) {
            dashMatches.forEach(m => addParsedPair(results, m[1], m[2], 'dash', qtyOrder));
            continue;
        }

        // Pattern 5: Space-separated pair on a single line (e.g. "3 19" or "100 20")
        const spacePair = line.match(/^\s*(\d+)\s+(\d+)\s*$/);
        if (spacePair) {
            addParsedPair(results, spacePair[1], spacePair[2], 'space', qtyOrder);
            continue;
        }

        // Pattern 6: Lone number — use stickyQty for 2-digit, default qty=1 for 4-digit
        const loneNum = line.match(/^\s*(\d{1,4})\s*$/);
        if (loneNum) {
            let num = loneNum[1];
            if (num.length === 1) num = '0' + num;
            if (num.length === 2 && stickyQty > 0) {
                results.push({ num, qty: stickyQty });
                continue;
            }
            if (num.length === 4) {
                results.push({ num, qty: stickyQty > 0 ? stickyQty : 1 });
                continue;
            }
        }

        // Pattern 7: Inline alternating numbers (e.g. "3104 2 5919 2 77 5 91 10")
        const tokens = line.match(/\d+/g);
        if (tokens && tokens.length >= 2 && tokens.length % 2 === 0) {
            for (let i = 0; i < tokens.length; i += 2) {
                addParsedPair(results, tokens[i], tokens[i + 1], 'inline', qtyOrder);
            }
        }
    }
    } // end groups loop

    return results;
}

function addParsedPair(results, left, right, format, qtyOrder) {
    let num, qty;
    const l = left.trim(), r = right.trim();

    if (format === 'equals') {
        // equals: already swapped in caller so left=qty, right=num
        qty = parseInt(l, 10); num = r;
    } else if (format === 'vil') {
        // vil/bil/de: always left=qty, right=num
        qty = parseInt(l, 10); num = r;
    } else {
        // dots, dash, slash, space, inline: use 4-digit detection + qtyOrder
        const lIs4 = l.length === 4;
        const rIs4 = r.length === 4;
        if (lIs4 && !rIs4) {
            num = l; qty = r ? parseInt(r, 10) : 1;
        } else if (rIs4 && !lIs4) {
            num = r; qty = l ? parseInt(l, 10) : 1;
        } else {
            if (qtyOrder === 'left') {
                qty = parseInt(l, 10); num = r;
            } else {
                num = l; qty = parseInt(r, 10);
            }
        }
    }

    // Pad 1-digit numbers to 2 digits
    if (num.length === 1) num = '0' + num;

    // Validate: num must be 2 or 4 digits, qty >= 1
    if ((num.length === 2 || num.length === 4) && /^\d+$/.test(num) && qty >= 1) {
        results.push({ num, qty });
    }
}

// --- END PASTE LIST FEATURE ---

window.mergeDuplicateNumbers = function () {
    if (!currentState.items.length) {
        showError("No hay números para juntar");
        return;
    }

    const mergedByNum = new Map();
    const orderedNums = [];
    let mergedCount = 0;

    currentState.items.forEach(item => {
        const num = String(item.num || "").trim();
        const qty = parseInt(item.qty, 10) || 0;
        if (!num || qty <= 0) return;

        if (!mergedByNum.has(num)) {
            mergedByNum.set(num, qty);
            orderedNums.push(num);
        } else {
            mergedByNum.set(num, mergedByNum.get(num) + qty);
            mergedCount++;
        }
    });

    currentState.items = orderedNums.map(num => {
        const qty = mergedByNum.get(num);
        const priceUnit = num.length === 4 ? 1.0 : 0.25;
        return { num, qty, totalLine: priceUnit * qty };
    });

    renderList();
    if (mergedCount > 0) showError(`Se juntaron ${mergedCount} repetidos`);
    else showError("No había repetidos");
};

window.deleteItem = function (index) {
    currentState.editingRowIndex = null;
    currentState.items.splice(index, 1); renderList();
};

window.clearAllItems = function () {
    if (currentState.items.length === 0) return;
    currentState.editingRowIndex = null;
    currentState.items = [];
    renderList();
};

window.startRowEdit = function (index) {
    currentState.editingRowIndex = index;
    renderList();
    setTimeout(() => {
        const numInput = document.getElementById('editNum' + index);
        if (numInput) numInput.focus();
    }, 50);
};

window.saveRowEdit = function (index) {
    const numInput = document.getElementById('editNum' + index);
    const qtyInput = document.getElementById('editQty' + index);
    const newNum = (numInput.value || '').replace(/[^0-9]/g, '');
    const newQty = parseInt((qtyInput.value || '').replace(/[^0-9]/g, ''));

    if (!newNum || (newNum.length !== 2 && newNum.length !== 4)) {
        numInput.style.borderColor = '#ff3b30';
        return;
    }
    if (!newQty || newQty <= 0) {
        qtyInput.style.borderColor = '#ff3b30';
        return;
    }

    const priceUnit = newNum.length === 2 ? 0.25 : 1.00;
    currentState.items[index] = { num: newNum, qty: newQty, totalLine: priceUnit * newQty };
    currentState.editingRowIndex = null;
    renderList();
};

window.cancelRowEdit = function () {
    currentState.editingRowIndex = null;
    renderList();
};
function showError(msg) { document.getElementById('errorMsg').innerText = msg; }

function renderList() {
    const listDiv = document.getElementById('itemsList');
    listDiv.innerHTML = "";
    let grandTotal = 0;
    currentState.items.forEach((item, index) => {
        const div = document.createElement('div');
        if (item.separator) {
            div.className = 'item-row separator-row';
            div.innerHTML = `<span style="flex:1; text-align:center; color:#999; font-size:14px;">─────────────────────────</span><button class="delete-btn" onclick="deleteItem(${index})">QUITAR</button>`;
        } else if (currentState.editingRowIndex === index) {
            div.className = 'item-row-edit';
            div.innerHTML = `
                <input type="tel" class="edit-num-input" id="editNum${index}" value="${item.num}" pattern="[0-9]*" inputmode="numeric">
                <input type="tel" class="edit-qty-input" id="editQty${index}" value="${item.qty}" pattern="[0-9]*" inputmode="numeric">
                <button class="edit-save-btn" onclick="saveRowEdit(${index})">OK</button>
                <button class="edit-cancel-btn" onclick="cancelRowEdit()">✕</button>
            `;
        } else {
            div.className = 'item-row';
            div.innerHTML = `<button class="item-tap-area" onclick="startRowEdit(${index})"><span class="item-num">*${item.num}*</span></button><button class="item-tap-area" onclick="startRowEdit(${index})">${item.qty}</button><span>${item.totalLine.toFixed(2)}</span><button class="delete-btn" onclick="deleteItem(${index})">QUITAR</button>`;
        }
        listDiv.appendChild(div);
        grandTotal += item.totalLine;
    });
    document.getElementById('grandTotal').innerText = "$" + grandTotal.toFixed(2);
    const clearBtn = document.getElementById('clearAllBtn');
    if (clearBtn) clearBtn.style.display = currentState.items.length > 0 ? '' : 'none';
    if (currentState.items.length > 0) {
        const btnLabel = currentState.editingTicketId
            ? `GUARDAR CAMBIOS ($${grandTotal.toFixed(2)})`
            : `IMPRIMIR ($${grandTotal.toFixed(2)})`;
        tg.MainButton.setText(btnLabel);
        tg.MainButton.show(); tg.MainButton.enable();
    } else {
        tg.MainButton.hide();
    }
    const paper = document.querySelector('.receipt-paper');
    if (paper) setTimeout(() => { paper.scrollTop = paper.scrollHeight; }, 50);
}

// 🟢 FIXED: Generates dates including TOMORROW and auto-scrolls to TODAY
window.editTicket = function (ticketId, lotteryType, date) {
    const ticket = currentState.history.tickets.find(t => t.id === ticketId);
    if (!ticket) { tg.showAlert("Ticket no encontrado"); return; }

    currentState.editingTicketId = ticketId;
    currentState.editingRowIndex = null;
    currentState.mode = 'user';
    currentState.lockedDraftContext = null;
    currentState.lottery = lotteryType;
    currentState.date = date;
    currentState.items = (ticket.items || []).map(item => item.separator ? { num: '---', qty: 0, totalLine: 0, separator: true } : ({
        num: String(item.num),
        qty: Number(item.qty),
        totalLine: Number(item.totalLine)
    }));

    syncInputHeader();
    showPage('page-input');
    renderList();
    setTimeout(() => {
        const ni = document.getElementById('numInput');
        if (ni) ni.focus();
    }, 300);
};

function initHistoryView(panamaNow) {
    const dates = [];

    // Loop from 6 days ago (i=6) up to Tomorrow (i=-1)
    for (let i = 6; i >= -1; i--) {
        const d = new Date(panamaNow);
        d.setDate(d.getDate() - i);
        const year = d.getFullYear();
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        dates.push(`${year}-${month}-${day}`);
    }

    // Calculate Today's String to set as default
    const pYear = panamaNow.getFullYear();
    const pMonth = String(panamaNow.getMonth() + 1).padStart(2, '0');
    const pDay = String(panamaNow.getDate()).padStart(2, '0');
    const todayStr = `${pYear}-${pMonth}-${pDay}`;

    // Decide which date to activate (Today if available, else the last one)
    const targetDate = dates.includes(todayStr) ? todayStr : dates[dates.length - 1];
    currentState.historyDate = targetDate;

    // Render Shelf with the target date active
    renderHistoryShelf(dates, targetDate);

    // Initial Load of Grid
    currentState.historyLottery = null;
    renderHistoryLotteryGrid(targetDate);
    renderHistoryTickets(targetDate, null);
}

function resolveIconSrc(iconPath) {
    try { return new URL(iconPath, ASSET_BASE).href; } catch (e) { return iconPath; }
}

function buildIconHtml(icon) {
    if (!icon) return `<span class="card-icon"></span>`;
    if (typeof icon === "string" && icon.toLowerCase().endsWith(".png")) {
        const iconSrc = resolveIconSrc(icon);
        return `<img class="card-flag" src="${iconSrc}" alt="">`;
    }
    return `<span class="card-icon">${icon}</span>`;
}

// 🟢 FIXED: Adds auto-scroll logic
function renderHistoryShelf(dates, activeDateStr) {
    const shelf = document.getElementById('historyShelf');
    shelf.innerHTML = "";
    let activeChipElement = null;

    dates.forEach((dateStr) => {
        const chip = document.createElement('div');
        const isActive = dateStr === activeDateStr;

        chip.className = `shelf-date ${isActive ? 'active' : ''}`;

        // Optional: Simple Label Logic
        let label = dateStr;

        chip.innerText = label;

        chip.onclick = () => {
            document.querySelectorAll('.shelf-date').forEach(c => c.classList.remove('active'));
            chip.classList.add('active');
            currentState.historyDate = dateStr;
            currentState.historyLottery = null;
            renderHistoryLotteryGrid(dateStr);
            renderHistoryTickets(dateStr, null);

            // Center clicked item
            chip.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
        };

        shelf.appendChild(chip);
        if (isActive) activeChipElement = chip;
    });

    // 🚀 MAGIC FIX: Scroll to the active date (Today) on load
    if (activeChipElement) {
        setTimeout(() => {
            activeChipElement.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
        }, 100);
    }
}

// 🟢 UPDATED: Change Text Logic Here
function renderHistoryLotteryGrid(dateStr) {
    const grid = document.getElementById('historyLotteryGrid');
    grid.innerHTML = "";
    const types = getHistoryLotteryTypes(dateStr);

    // 🟢 CHANGED: New Text "No compraste para esta fecha"
    if (types.length === 0) {
        grid.innerHTML = "<div style='grid-column: span 2; text-align: center; color: #888; padding: 20px; font-weight: 500;'>No compraste para esta fecha</div>";
        return;
    }

    types.forEach(lotteryType => {
        const meta = getLotteryMetaFromType(lotteryType);
        const card = document.createElement('div');
        card.className = "lottery-card";
        if (meta.special) card.classList.add('card-nacional');
        const isSelected = currentState.historyLottery === lotteryType && currentState.historyDate === dateStr;
        if (isSelected && !meta.special) {
            card.style.border = "2px solid #3390ec"; card.style.background = "#f0f8ff";
        }
        const iconHtml = buildIconHtml(meta.icon);
        card.innerHTML = `${iconHtml}<div class="card-name">${meta.name}</div><div class="card-time">${meta.time}</div>`;
        card.onclick = () => selectHistoryLottery(lotteryType, dateStr);
        grid.appendChild(card);
    });
}

function selectHistoryLottery(lotteryType, dateStr) {
    currentState.historyLottery = lotteryType;
    renderHistoryLotteryGrid(dateStr);
    renderHistoryTickets(dateStr, lotteryType);
}

function renderHistoryTickets(dateStr, lotteryType) {
    const list = document.getElementById('historyList');
    list.innerHTML = "";
    if (!lotteryType) {
        list.innerHTML = "<div style='text-align:center;color:#888;padding:10px;'>Selecciona un sorteo.</div>";
        return;
    }
    const tickets = currentState.history.tickets.filter(t => t.date === dateStr && t.lottery_type === lotteryType);
    if (tickets.length === 0) {
        list.innerHTML = "<div style='text-align:center;color:#888;padding:10px;'>No hay tickets para este sorteo.</div>";
        return;
    }

    const buildStatusBadge = (text, className, inlineStyle) => {
        const badge = document.createElement('span');
        badge.className = className || 'h-status';
        if (inlineStyle) badge.style.cssText = inlineStyle;
        badge.textContent = text;
        return badge;
    };

    tickets.forEach(ticket => {
        const resultsKey = `${ticket.date}|${ticket.lottery_type}`;
        const results = currentState.history.results[resultsKey];
        const status = ticket.status || "PENDING";
        const nums = (ticket.items || []).map(i => `${i.num} x${i.qty}`).join(" | ");
        const card = document.createElement('div');

        let statusBadge = null;
        let checkedBadge = null;
        let breakdownLines = [];
        let cardExtraClass = "";
        let showDeleteBtn = true;

        if (status === 'DELETED') {
            statusBadge = buildStatusBadge("ELIMINADO", "h-status status-deleted");
            cardExtraClass = "card-deleted";
            showDeleteBtn = false;
        } else if (status === 'INVALID') {
            statusBadge = buildStatusBadge("ANULADO", "h-status status-invalid");
            cardExtraClass = "card-invalid";
            showDeleteBtn = false;
        } else if (results) {
            const calc = calculateTicketWin(ticket.items || [], results, ticket.lottery_type);
            checkedBadge = buildStatusBadge("Chequeado", "h-status", "background:#e5e5ea;color:#333;margin-left:8px;");
            if (calc.total > 0) {
                statusBadge = buildStatusBadge(`Ganaste $${calc.total.toFixed(2)}`, "h-status status-win");
                breakdownLines = calc.lines || [];
            } else {
                statusBadge = buildStatusBadge("No ganó", "h-status status-loss");
            }
        } else {
            statusBadge = buildStatusBadge("Pendiente de introducir premios", "h-status status-wait");
        }

        card.className = `history-card ${cardExtraClass}`;
        card.id = `ticket-card-${ticket.id}`;

        const header = document.createElement('div');
        header.className = 'h-header';
        const dateDiv = document.createElement('div');
        dateDiv.textContent = ticket.date;
        const idDiv = document.createElement('div');
        idDiv.textContent = `Ticket #${ticket.id}`;
        header.appendChild(dateDiv);
        header.appendChild(idDiv);
        card.appendChild(header);

        const title = document.createElement('div');
        title.className = 'h-title';
        title.textContent = ticket.lottery_type;
        card.appendChild(title);

        const numsDiv = document.createElement('div');
        numsDiv.className = 'h-nums';
        numsDiv.textContent = nums || "-";
        card.appendChild(numsDiv);

        const statusRow = document.createElement('div');
        if (statusBadge) statusRow.appendChild(statusBadge);
        if (checkedBadge) statusRow.appendChild(checkedBadge);
        card.appendChild(statusRow);

        if (breakdownLines.length > 0) {
            const breakdown = document.createElement('div');
            breakdown.className = 'h-breakdown';
            breakdownLines.forEach((line, index) => {
                if (index > 0) breakdown.appendChild(document.createElement('br'));
                breakdown.appendChild(document.createTextNode(line));
            });
            card.appendChild(breakdown);
        }

        const actions = document.createElement('div');
        actions.className = 'h-actions';
        if (showDeleteBtn) {
            if (!results) {
                const editButton = document.createElement('button');
                editButton.className = 'h-edit-btn';
                editButton.textContent = '✏️ Editar';
                editButton.addEventListener('click', function (event) {
                    event.stopPropagation();
                    editTicket(ticket.id, ticket.lottery_type, ticket.date);
                });
                actions.appendChild(editButton);
            }

            const deleteButton = document.createElement('button');
            deleteButton.className = 'h-delete-btn';
            deleteButton.textContent = '🗑️ Eliminar';
            deleteButton.addEventListener('click', function (event) {
                event.stopPropagation();
                startDeleteTicket(ticket.id);
            });
            actions.appendChild(deleteButton);
        }

        card.appendChild(actions);
        list.appendChild(card);
    });
    return;
}

// 🗑️ DELETE TICKET FLOW (Modal Popup)
let pendingDeleteTicketId = null;

window.startDeleteTicket = function (ticketId) {
    pendingDeleteTicketId = ticketId;
    document.getElementById('deleteModalText').innerHTML =
        `¿Seguro que deseas eliminar<br><b>Ticket #${ticketId}</b>?`;
    document.getElementById('deleteModal').classList.remove('hidden');
};

window.cancelDelete = function () {
    pendingDeleteTicketId = null;
    document.getElementById('deleteModal').classList.add('hidden');
};

window.confirmDelete = function () {
    if (!pendingDeleteTicketId) return;
    const ticketId = pendingDeleteTicketId;

    // Close modal immediately
    document.getElementById('deleteModal').classList.add('hidden');
    pendingDeleteTicketId = null;

    // Build auth data
    let authData = "";
    if (tg.initDataUnsafe && tg.initDataUnsafe.user) {
        authData = "FORCE_ID_" + tg.initDataUnsafe.user.id;
    } else {
        const urlParams = new URLSearchParams(window.location.search);
        const forcedUid = urlParams.get('uid');
        if (forcedUid) authData = "FORCE_ID_" + forcedUid;
    }

    fetch(`${API_URL}/delete_ticket`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData: authData, ticket_id: ticketId })
    })
        .then(res => res.json())
        .then(resp => {
            if (resp.ok) {
                // Update local state
                const ticket = currentState.history.tickets.find(t => t.id === ticketId);
                if (ticket) ticket.status = 'DELETED';

                // Update UI
                const card = document.getElementById(`ticket-card-${ticketId}`);
                if (card) {
                    card.classList.add('card-deleted');
                    // Replace ALL status badges
                    card.querySelectorAll('.h-status').forEach(badge => badge.remove());
                    const deletedBadge = document.createElement('span');
                    deletedBadge.className = 'h-status status-deleted';
                    deletedBadge.innerText = 'ELIMINADO';
                    const statusContainer = card.querySelector('.h-nums');
                    if (statusContainer) statusContainer.after(deletedBadge);
                    else card.prepend(deletedBadge);
                    // Remove breakdown if present
                    const breakdown = card.querySelector('.h-breakdown');
                    if (breakdown) breakdown.remove();
                    // Remove delete button
                    const delBtn = card.querySelector('.h-delete-btn');
                    if (delBtn) delBtn.remove();

                    // Show refund if applicable
                    if (resp.refunded && resp.refunded > 0) {
                        const refundEl = document.createElement('div');
                        refundEl.style.cssText = 'font-size:13px; color:#2e7d32; margin-top:5px; font-weight:600;';
                        refundEl.innerText = `💰 Reembolso: $${resp.refunded.toFixed(2)}`;
                        card.appendChild(refundEl);
                    }
                }
            } else {
                alert("Error: " + (resp.error || "No se pudo eliminar"));
            }
        })
        .catch(err => {
            alert("Error de conexión: " + err.message);
        });
};

function setHistoryStatus(text) {
    const el = document.getElementById('historyStatus');
    if (!el) return;
    el.innerText = text || "";
    el.style.display = text ? 'block' : 'none';
}

function showDebugUrl() {
    const el = document.getElementById('historyDebugUrl');
    if (!el) return;
    el.innerText = window.location.href;
}

function renderErrorMessage(container, text) {
    if (!container) return;
    container.innerHTML = "";
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error';
    errorDiv.textContent = text || "Error";
    container.appendChild(errorDiv);
}

function getHistoryLotteryTypes(dateStr) {
    const types = new Set();
    currentState.history.tickets.filter(t => t.date === dateStr && t.lottery_type).forEach(t => types.add(t.lottery_type));
    const ordered = [];
    const knownOrder = [NACIONAL_LOTTERY, ...STANDARD_LOTTERIES].map(l => `${l.name} ${l.time}`);
    knownOrder.forEach(type => { if (types.has(type)) ordered.push(type); types.delete(type); });
    Array.from(types).sort().forEach(type => ordered.push(type));
    return ordered;
}

function getLotteryMetaFromType(lotteryType) {
    const known = [NACIONAL_LOTTERY, ...STANDARD_LOTTERIES].find(lot => `${lot.name} ${lot.time}` === lotteryType);
    if (known) {
        return { name: known.name, time: known.time, icon: known.icon, special: !!known.special };
    }
    const parts = lotteryType.split(' ');
    const time = parts.length >= 2 ? parts.slice(-2).join(' ') : "";
    const name = parts.length >= 3 ? parts.slice(0, -2).join(' ') : lotteryType;
    let icon = "";
    if (name.includes("Nacional")) icon = "🇵🇦";
    else if (name.includes("Tica")) icon = "🇨🇷";
    else if (name.includes("Nica")) icon = "🇳🇮";
    else if (name.includes("Primera")) icon = "🇩🇴";
    return { name, time, icon, special: name.includes("Nacional") };
}

function calculateTicketWin(items, results, lotteryType) {
    const w1 = String(results.w1 || "");
    const w2 = String(results.w2 || "");
    const w3 = String(results.w3 || "");
    const isNacional = (lotteryType || "").includes("Nacional");
    const win4_12 = w1 + w2; const win4_13 = w1 + w3; const win4_23 = w2 + w3;
    let total = 0; const lines = [];
    items.forEach(item => {
        const num = String(item.num || ""); const bet = Number(item.qty || 0);
        if (num.length === 2) {
            if (isNacional) {
                if (w1.length >= 2 && num === w1.slice(-2)) { const win = bet * 14.00; total += win; lines.push(`Chances (1er): $14.00 * ${bet} = $${win.toFixed(2)}`); }
                if (w2.length >= 2 && num === w2.slice(-2)) { const win = bet * 3.00; total += win; lines.push(`Chances (2do): $3.00 * ${bet} = $${win.toFixed(2)}`); }
                if (w3.length >= 2 && num === w3.slice(-2)) { const win = bet * 2.00; total += win; lines.push(`Chances (3er): $2.00 * ${bet} = $${win.toFixed(2)}`); }
            } else {
                if (num === w1) { const win = bet * AWARDS['2_digit_1']; total += win; lines.push(`1er Premio: $${AWARDS['2_digit_1']} * ${bet} = $${win.toFixed(2)}`); }
                if (num === w2) { const win = bet * AWARDS['2_digit_2']; total += win; lines.push(`2do Premio: $${AWARDS['2_digit_2']} * ${bet} = $${win.toFixed(2)}`); }
                if (num === w3) { const win = bet * AWARDS['2_digit_3']; total += win; lines.push(`3er Premio: $${AWARDS['2_digit_3']} * ${bet} = $${win.toFixed(2)}`); }
            }
        } else if (num.length === 4) {
            if (isNacional) {
                if (w1.length === 4) {
                    if (num === w1) { const win = bet * 2000; total += win; lines.push(`1er Premio (Exacto): $2000 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(0,3) === w1.slice(0,3)) { const win = bet * 50; total += win; lines.push(`1er Premio (3 Primeras): $50 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-3) === w1.slice(-3)) { const win = bet * 50; total += win; lines.push(`1er Premio (3 Ultimas): $50 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(0,2) === w1.slice(0,2)) { const win = bet * 3; total += win; lines.push(`1er Premio (2 Primeras): $3 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-2) === w1.slice(-2)) { const win = bet * 3; total += win; lines.push(`1er Premio (2 Ultimas): $3 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-1) === w1.slice(-1)) { const win = bet * 1; total += win; lines.push(`1er Premio (Ultima): $1 * ${bet} = $${win.toFixed(2)}`); }
                }
                if (w2.length === 4) {
                    if (num === w2) { const win = bet * 600; total += win; lines.push(`2do Premio (Exacto): $600 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(0,3) === w2.slice(0,3)) { const win = bet * 20; total += win; lines.push(`2do Premio (3 Primeras): $20 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-3) === w2.slice(-3)) { const win = bet * 20; total += win; lines.push(`2do Premio (3 Ultimas): $20 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-2) === w2.slice(-2)) { const win = bet * 2; total += win; lines.push(`2do Premio (2 Ultimas): $2 * ${bet} = $${win.toFixed(2)}`); }
                }
                if (w3.length === 4) {
                    if (num === w3) { const win = bet * 300; total += win; lines.push(`3er Premio (Exacto): $300 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(0,3) === w3.slice(0,3)) { const win = bet * 10; total += win; lines.push(`3er Premio (3 Primeras): $10 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-3) === w3.slice(-3)) { const win = bet * 10; total += win; lines.push(`3er Premio (3 Ultimas): $10 * ${bet} = $${win.toFixed(2)}`); }
                    else if (num.slice(-2) === w3.slice(-2)) { const win = bet * 1; total += win; lines.push(`3er Premio (2 Ultimas): $1 * ${bet} = $${win.toFixed(2)}`); }
                }
            } else {
                if (num === win4_12) { const win = bet * AWARDS['4_digit_12']; total += win; lines.push(`Billete 1ro/2do: $${AWARDS['4_digit_12']} * ${bet} = $${win.toFixed(2)}`); }
                if (num === win4_13) { const win = bet * AWARDS['4_digit_13']; total += win; lines.push(`Billete 1ro/3ro: $${AWARDS['4_digit_13']} * ${bet} = $${win.toFixed(2)}`); }
                if (num === win4_23) { const win = bet * AWARDS['4_digit_23']; total += win; lines.push(`Billete 2do/3ro: $${AWARDS['4_digit_23']} * ${bet} = $${win.toFixed(2)}`); }
            }
        }
    });
    return { total, lines };
}

function populateAdminSelect() {
    const sel = document.getElementById('adminLotterySelect');
    if (!sel) return;
    const adminDateEl = document.getElementById('adminDate');
    const selectedDate = adminDateEl && adminDateEl.value ? adminDateEl.value : currentState.date;
    const previousValue = sel.value;

    sel.innerHTML = "";
    const allLotteries = [...STANDARD_LOTTERIES];
    if (selectedDate && currentState.activeNacionalDates.includes(selectedDate)) {
        allLotteries.push(NACIONAL_LOTTERY);
    }

    allLotteries.forEach(lot => {
        const opt = document.createElement('option');
        opt.value = lot.name + " " + lot.time;
        opt.innerText = lot.name + " " + lot.time;
        sel.appendChild(opt);
    });

    if (previousValue && Array.from(sel.options).some(o => o.value === previousValue)) {
        sel.value = previousValue;
    }

    applyAdminAwardInputRules();
}

function setupAdminDashboardListeners() {
    const adminDateEl = document.getElementById('adminDate');
    const adminLotteryEl = document.getElementById('adminLotterySelect');

    if (adminDateEl) {
        adminDateEl.addEventListener('change', () => {
            populateAdminSelect();
        });
    }

    if (adminLotteryEl) {
        adminLotteryEl.addEventListener('change', () => {
            applyAdminAwardInputRules();
        });
    }
}

function applyAdminAwardInputRules() {
    const lot = (document.getElementById('adminLotterySelect') || {}).value || "";
    const isNacional = lot.includes("Nacional");
    const maxLen = isNacional ? 4 : 2;
    const placeholder = isNacional ? "0000" : "00";

    ['w1', 'w2', 'w3'].forEach(id => {
        const input = document.getElementById(id);
        if (!input) return;
        input.maxLength = maxLen;
        input.placeholder = placeholder;
        input.value = (input.value || "").replace(/\D/g, "").slice(0, maxLen);
    });
}

// 🟢 ADMIN FUNCTIONS
window.openAdminResults = function () {
    currentState.mode = 'admin';
    showPage('page-admin');
    const adminDateEl = document.getElementById('adminDate');
    if (adminDateEl && !adminDateEl.value) {
        adminDateEl.value = currentState.date;
    }
    populateAdminSelect();
};

window.saveResults = function () {
    const date = document.getElementById('adminDate').value;
    const lot = document.getElementById('adminLotterySelect').value;
    const w1 = document.getElementById('w1').value.trim();
    const w2 = document.getElementById('w2').value.trim();
    const w3 = document.getElementById('w3').value.trim();
    if (!w1 || !w2 || !w3) { tg.showAlert("⚠️ 缺少开奖号码"); return; }

    const isNacional = lot.includes("Nacional");
    const expectedLen = isNacional ? 4 : 2;
    const validRegex = new RegExp(`^\\d{${expectedLen}}$`);
    if (!validRegex.test(w1) || !validRegex.test(w2) || !validRegex.test(w3)) {
        tg.showAlert(isNacional ? "⚠️ Nacional需要4位开奖号码" : "⚠️ 该Sorteo需要2位开奖号码");
        return;
    }

    const payload = { action: 'save_results', date: date, lottery: lot, w1: w1, w2: w2, w3: w3 };
    tg.sendData(JSON.stringify(payload));
};

tg.MainButton.onClick(function () {
    if (currentState.mode === 'admin' || currentState.mode === 'history') return;
    if (currentState.items.length === 0) return;
    const modal = document.getElementById('reviewModal');
    if (modal) { modal.classList.remove('hidden'); }
    else { tg.showAlert("Error: Modal HTML missing. Update index.html"); }
});

window.closeReview = function () { document.getElementById('reviewModal').classList.add('hidden'); }

window.confirmPrint = function () {
    const btn = document.querySelector('.modal-btn.confirm');
    if (btn) { btn.disabled = true; btn.innerText = "Guardando..."; }

    // Build auth data (same pattern as confirmDelete)
    let authData = "";
    if (tg.initDataUnsafe && tg.initDataUnsafe.user) {
        authData = "FORCE_ID_" + tg.initDataUnsafe.user.id;
    } else {
        const urlParams = new URLSearchParams(window.location.search);
        const forcedUid = urlParams.get('uid');
        if (forcedUid) authData = "FORCE_ID_" + forcedUid;
    }

    if (currentState.editingTicketId) {
        // EDIT MODE: Update existing ticket via API, then trigger reprint
        fetch(`${API_URL}/edit_ticket`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                initData: authData,
                ticket_id: currentState.editingTicketId,
                items: currentState.items
            })
        })
        .then(res => res.json())
        .then(resp => {
            if (!resp.ok) {
                tg.showAlert("Error: " + (resp.error || "No se pudo editar"));
                if (btn) { btn.disabled = false; btn.innerText = "✅ Revisado"; }
                return;
            }
            tg.sendData(JSON.stringify({ action: 'print_ticket', ticket_id: resp.ticket_id, is_edit: true }));
            setTimeout(() => { tg.close(); }, 500);
        })
        .catch(err => {
            tg.showAlert("Error de conexión: " + err.message);
            if (btn) { btn.disabled = false; btn.innerText = "✅ Revisado"; }
        });
    } else {
        // CREATE MODE: Save new ticket via API, then trigger print
        const lockedContext = currentState.lockedDraftContext || {};
        fetch(`${API_URL}/save_ticket`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                initData: authData,
                date: lockedContext.date || currentState.date,
                lottery_type: lockedContext.lottery || currentState.lottery,
                items: currentState.items
            })
        })
        .then(res => res.json())
        .then(resp => {
            if (!resp.ok) {
                tg.showAlert("Error: " + (resp.error || "No se pudo guardar"));
                if (btn) { btn.disabled = false; btn.innerText = "✅ Revisado"; }
                return;
            }
            const printPayload = { action: 'print_ticket', ticket_id: resp.ticket_id };
            if (currentState.draftId) {
                printPayload.draft_id = currentState.draftId;
            }
            tg.sendData(JSON.stringify(printPayload));
            setTimeout(() => { tg.close(); }, 500);
        })
        .catch(err => {
            tg.showAlert("Error de conexión: " + err.message);
            if (btn) { btn.disabled = false; btn.innerText = "✅ Revisado"; }
        });
    }
}

// 🟢 STATS LOGIC (Merged from Source Bot)
window.goToStats = function () {
    showPage('page-stats-menu');
    initStatsView();
}

// 🟢 FIXED: Now passes 'defaultDate' so the Shelf highlights the correct day
window.initStatsView = function () {
    const dates = [];
    const panamaNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Panama" }));

    // Loop from -1 (Tomorrow) to 10 days ago
    for (let i = -1; i < 10; i++) {
        const d = new Date(panamaNow);
        d.setDate(d.getDate() - i);
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        dates.push(`${y}-${m}-${day}`);
    }

    // Calculate Today's String
    const pYear = panamaNow.getFullYear();
    const pMonth = String(panamaNow.getMonth() + 1).padStart(2, '0');
    const pDay = String(panamaNow.getDate()).padStart(2, '0');
    const todayStr = `${pYear}-${pMonth}-${pDay}`;

    // Default to Today if available, otherwise Tomorrow
    const defaultDate = dates.includes(todayStr) ? todayStr : dates[0];

    // 🟢 PASS defaultDate to renderStatsShelf
    renderStatsShelf(dates, defaultDate);
    selectStatsDate(defaultDate);
}

// 🟢 FIXED: Accepts 'activeDateStr' to highlight the REAL active date
window.renderStatsShelf = function (dates, activeDateStr) {
    const shelf = document.getElementById('statsShelf');
    shelf.innerHTML = "";

    dates.forEach((d) => {
        const chip = document.createElement('div');
        // Only add 'active' if it matches the logic (not just index 0)
        const isActive = d === activeDateStr;
        chip.className = `shelf-date ${isActive ? 'active' : ''}`;
        chip.innerText = d;

        chip.onclick = () => {
            document.querySelectorAll('#statsShelf .shelf-date').forEach(e => e.classList.remove('active'));
            chip.classList.add('active');
            selectStatsDate(d);
        };
        shelf.appendChild(chip);

        // Auto-scroll to active
        if (isActive) {
            setTimeout(() => {
                chip.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
            }, 100);
        }
    });
}

// 🟢 FIXED: Puts Nacional on TOP using 'unshift' & Correct Timezone Logic
window.selectStatsDate = function (dateStr) {
    currentState.statsDate = dateStr;
    const grid = document.getElementById('statsLotteryGrid');
    grid.innerHTML = "";

    // Hybrid Check for Nacional Visibility
    let showNacional = currentState.activeNacionalDates.includes(dateStr);

    // 🟢 TIMEZONE SAFE CHECK for History
    if (!showNacional) {
        const d = new Date(dateStr + "T12:00:00");
        const day = d.getDay(); // 0 = Sun, 3 = Wed

        // Robust Panama Today Calculation (Matches initStatsView)
        const panamaNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Panama" }));
        const pYear = panamaNow.getFullYear();
        const pMonth = String(panamaNow.getMonth() + 1).padStart(2, '0');
        const pDay = String(panamaNow.getDate()).padStart(2, '0');
        const pTodayStr = `${pYear}-${pMonth}-${pDay}`;

        if (dateStr < pTodayStr && (day === 0 || day === 3)) {
            showNacional = true;
        }
    }

    let all = [...STANDARD_LOTTERIES];
    if (showNacional) {
        // 🟢 FIX 1: UNSHIFT puts it at the TOP (Start of array)
        all.unshift(NACIONAL_LOTTERY);
    }

    all.forEach(lot => {
        const card = document.createElement('div');
        card.className = "lottery-card";
        if (lot.special) card.classList.add('card-nacional');
        card.innerHTML = `${buildIconHtml(lot.icon)}<div class="card-name">${lot.name}</div><div class="card-time">${lot.time}</div>`;
        card.onclick = () => loadDetailedStats(dateStr, lot.name + " " + lot.time);
        grid.appendChild(card);
    });
}

window.loadDetailedStats = function (date, lottery) {
    showPage('page-stats-detail');
    document.getElementById('statsDetailTitle').innerText = `${date} | ${lottery}`;
    const container = document.getElementById('statsDetailContent');
    container.innerHTML = "<div style='text-align:center; padding:20px;'>正在加载数据...</div>";

    // 🛑 FIX: Explicit routing for Stats too
    let authData = "";
    if (tg.initDataUnsafe && tg.initDataUnsafe.user) {
        authData = "FORCE_ID_" + tg.initDataUnsafe.user.id;
    } else {
        // Fallback to URL uid
        const urlParams = new URLSearchParams(window.location.search);
        const forcedUid = urlParams.get('uid');
        if (forcedUid) authData = "FORCE_ID_" + forcedUid;
    }

    fetch(`${API_URL}/admin/stats_detail`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData: authData, date: date, lottery: lottery })
    })
        .then(res => {
            if (!res.ok) throw new Error("服务器错误");
            return res.json();
        })
        .then(resp => {
            if (!resp.ok) {
                renderErrorMessage(container, resp.error);
                return;
            }
            renderDetailedTable(resp.data, container);
        })
        .catch(err => {
            renderErrorMessage(container, `连接错误: ${err.message}`);
        });
}

window.renderDetailedTable = function (data, container) {
    const s = data.sales;
    const p = data.payouts;
    const w = data.meta;

    const net = s.total - p.total_won;
    const netColor = net >= 0 ? '#2e7d32' : '#c62828';

    let html = `
        <div style="background:#fff; padding:15px; border-radius:10px; margin-bottom:15px; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
            <h3 style="margin:0 0 10px 0; font-size:16px;">统计</h3>
            <div style="display:flex; justify-content:space-between; margin-bottom:5px;"><span>总销量:</span> <b>$${s.total.toFixed(2)}</b></div>
            <div style="display:flex; justify-content:space-between; margin-bottom:5px;"><span>中奖额:</span> <b>$${p.total_won.toFixed(2)}</b></div>
            <div style="display:flex; justify-content:space-between; border-top:1px solid #eee; padding-top:5px; font-size:18px;">
                <span>利润:</span> <b style="color:${netColor}">$${net.toFixed(2)}</b>
            </div>
        </div>

        <div style="background:#fff; padding:15px; border-radius:10px; margin-bottom:15px;">
            <h3 style="margin:0 0 10px 0; font-size:16px;">销售额分类</h3>
            <div style="display:flex; justify-content:space-between;"><span>二位数:</span> <span>${s.chances_qty} ($${s.chances_amount.toFixed(2)})</span></div>
            <div style="display:flex; justify-content:space-between;"><span>四位数:</span> <span>${s.billetes_qty} ($${s.billetes_amount.toFixed(2)})</span></div>
        </div>
    `;

    // --- WINNERS SECTION ---
    if (!w.w1) {
        html += `<div style="text-align:center; color:#999;">还未输入开奖号码</div>`;
    } else {
        html += `<h3 style="padding-left:5px; margin-bottom:10px;">🏆 中奖明细</h3>`;

        // 🟢 SAFETY FIX HERE: Checks if 'paid' is strictly undefined
        const drawChanceRow = (label, num, statObj) => {
            const count = (statObj && statObj.count !== undefined) ? statObj.count : 0;
            const paid = (statObj && statObj.paid !== undefined) ? statObj.paid : 0;
            const numDisplay = num ? num.slice(-2) : "--";

            return `
            <div style="background:#fff; padding:10px; border-radius:8px; margin-bottom:8px; display:flex; align-items:center;">
                <div style="width:40px; font-weight:bold; font-size:18px;">${numDisplay}</div>
                <div style="flex:1; padding-left:10px;">
                    <div style="font-size:12px; color:#666;">${label}</div>
                    <div style="font-size:14px;"><b>${count}</b> 位中奖</div>
                </div>
                <div style="font-weight:bold; color:#c62828;">$${paid.toFixed(2)}</div>
            </div>`;
        };

        html += drawChanceRow("一等奖（两位）", w.w1, p.chances.w1);
        html += drawChanceRow("二等奖（两位）", w.w2, p.chances.w2);
        html += drawChanceRow("三等奖（两位）", w.w3, p.chances.w3);

        if (data.meta.type.includes("Nacional") && p.billetes) {
            html += `<h3 style="padding-left:5px; margin-top:20px; margin-bottom:10px;">🇵🇦 四位票中奖细分</h3>`;
            const catMap = {
                "Exacto": "整号",
                "3 Primeras": "前三位",
                "3 Ultimas": "后三位",
                "2 Primeras": "前两位",
                "2 Ultimas": "后两位",
                "Ultima": "最后一位"
            };
            if (p.billetes.w1) {
                for (const [cat, val] of Object.entries(p.billetes.w1)) {
                    // Safety for loop vars (though these are usually safe coming from Object.entries)
                    const safeCount = val.count || 0;
                    const safePaid = val.paid || 0;
                    const displayCat = catMap[cat] || cat;
                    html += `<div style="font-size:13px; display:flex; justify-content:space-between; padding:5px 10px; background:#fff; margin-bottom:2px;">
                        <span>一等奖 ${displayCat}:</span> <span><b>${safeCount}</b> ($${safePaid})</span>
                      </div>`;
                }
            }
        }
    }
    container.innerHTML = html;
}
