<template>
  <div class="chatbox">
    <div class="chat-messages" ref="messagesContainer">
      <transition-group name="fade-slide" tag="div">
        <div v-for="(msg, idx) in messages" :key="idx" :class="['message-bubble', msg.role]">
          <div class="bubble-content">
            <strong v-if="msg.role === 'user'">You:</strong>
            <strong v-else>Bot:</strong>
            <span>{{ msg.content }}</span>
          </div>
        </div>
      </transition-group>
    </div>
    <div v-if="loading" class="chat-loading">
      <span class="dot"></span>
      <span class="dot"></span>
      <span class="dot"></span>
    </div>
    <form @submit.prevent="sendMessage">
      <input
        v-model="input"
        type="text"
        placeholder="Type your message..."
        :disabled="loading"
        @keyup.enter="sendMessage"
      />
      <button type="submit" :disabled="loading || !input">Send</button>
    </form>
  </div>
</template>
<script>
import { store } from '../Globals.js'
import { baseWidget } from './baseWidget.js'

export default {
    name: 'ChatBox',
    mixins: [baseWidget],
    data () {
        return {
            input: '',
            messages: [],
            loading: false,
            state: store,
            width: 320,
            height: 215,
            left: 768,
            top: 0,
            forceRecompute: 0
        }
    },
    methods: {
        async sendMessage () {
            if (!this.input.trim()) return
            const userMsg = { role: 'user', content: this.input }

            this.messages.push(userMsg)
            const userInput = this.input
            this.input = ''
            this.loading = true
            try {
                const response = await fetch('http://localhost:8001/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: userInput, sessionId: this.state.chatBotSessionID })
                })
                const data = await response.json()
                this.messages.push({ role: 'bot', content: data.response })
            } catch (e) {
                this.messages.push({ role: 'bot', content: 'Something went wrong. Please try again.' })
            }
            this.loading = false
        }
    }
}
</script>

<style scoped>
.chatbox {
    position: fixed;
    bottom: 20px;
    right: 20px;
    width: 340px;
    background: #f9fafd;
    border: 1px solid #dbeafe;
    border-radius: 14px;
    box-shadow: 0 4px 16px rgba(0,0,0,0.10);
    z-index: 1000;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}
.chat-messages {
    max-height: 260px;
    overflow-y: auto;
    padding: 16px 10px 10px 10px;
    flex: 1;
    background: #f9fafd;
}
.message-bubble {
    display: flex;
    flex-direction: column;
    max-width: 85%;
    margin-bottom: 10px;
    padding: 8px 14px;
    border-radius: 18px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    font-size: 15px;
    opacity: 1;
    transition: box-shadow 0.2s;
}
.message-bubble.user {
    align-self: flex-end;
    background: linear-gradient(135deg, #dbeafe 0%, #93c5fd 100%);
    color: #1e293b;
    border-bottom-right-radius: 4px;
}
.message-bubble.bot {
    align-self: flex-start;
    background: #fff;
    color: #334155;
    border-bottom-left-radius: 4px;
}
.bubble-content strong {
    font-size: 12px;
    color: #64748b;
    margin-right: 6px;
}
form {
    display: flex;
    border-top: 1px solid #e0e7ef;
    background: #f1f5f9;
    padding: 8px 8px 8px 12px;
}
input[type="text"] {
    flex: 1;
    border: none;
    padding: 10px 12px;
    border-radius: 8px;
    outline: none;
    font-size: 15px;
    background: #fff;
    margin-right: 8px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    transition: box-shadow 0.2s;
}
input[type="text"]:focus {
    box-shadow: 0 2px 8px rgba(59,130,246,0.10);
}
button {
    border: none;
    background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
    color: #fff;
    padding: 0 20px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 15px;
    font-weight: 500;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    transition: background 0.2s;
}
button:disabled {
    background: #cbd5e1;
    cursor: not-allowed;
}
.chat-loading {
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 8px 0;
    min-height: 24px;
}
.dot {
    height: 10px;
    width: 10px;
    margin: 0 3px;
    background-color: #3b82f6;
    border-radius: 50%;
    display: inline-block;
    animation: chat-dot-bounce 1.4s infinite both;
}
.dot:nth-child(2) {
    animation-delay: 0.2s;
}
.dot:nth-child(3) {
    animation-delay: 0.4s;
}
@keyframes chat-dot-bounce {
    0%, 80%, 100% {
        transform: scale(0.7);
        opacity: 0.7;
    }
    40% {
        transform: scale(1.2);
        opacity: 1;
    }
}
/* Fade-slide animation for new messages */
.fade-slide-enter-active {
    transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1);
}
.fade-slide-leave-active {
    transition: opacity 0.2s;
}
.fade-slide-enter-from {
    opacity: 0;
    transform: translateY(16px);
}
.fade-slide-enter-to {
    opacity: 1;
    transform: translateY(0);
}
.fade-slide-leave-from {
    opacity: 1;
}
.fade-slide-leave-to {
    opacity: 0;
}
</style>
