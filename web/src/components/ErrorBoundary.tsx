import React, { Component, ErrorInfo, ReactNode } from 'react';
import { AlertTriangle, RefreshCw } from 'lucide-react';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  errorMessage: string;
}

export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    errorMessage: '',
  };

  public static getDerivedStateFromError(error: Error): State {
    return {
      hasError: true,
      errorMessage: error.message || 'Error inesperado de renderizado en la interfaz.',
    };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error('SismoWatch UI ErrorBoundary caught error:', error, errorInfo);
  }

  private handleReload = (): void => {
    this.setState({ hasError: false, errorMessage: '' });
    window.location.reload();
  };

  public render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-slate-950 text-slate-100 flex items-center justify-center p-6">
          <div className="max-w-md w-full bg-slate-900 border border-red-500/40 rounded-2xl p-8 text-center shadow-2xl">
            <div className="w-14 h-14 rounded-full bg-red-500/15 border border-red-500/30 flex items-center justify-center mx-auto mb-4">
              <AlertTriangle className="w-7 h-7 text-red-400" />
            </div>
            <h1 className="text-xl font-bold text-white mb-2">
              Protección de Interfaz SismoWatch Activada
            </h1>
            <p className="text-sm text-slate-400 mb-6">
              Se interceptó un fallo inesperado para evitar una pantalla en blanco:{' '}
              <span className="text-red-300 font-mono">{this.state.errorMessage}</span>
            </p>
            <button
              onClick={this.handleReload}
              className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-orange-600 hover:bg-orange-500 text-white font-semibold text-sm transition"
            >
              <RefreshCw className="w-4 h-4" />
              Reintentar y Recargar Vista
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
