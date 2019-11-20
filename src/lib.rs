// Copyright (c) 2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Create a [`Sink`] implementation from an initial value and a closure
//! returning a [`Future`].
//!
//! This is very similar to how `futures::stream::unfold` creates a `Stream`
//! implementation from a seed value and a future-returning closure.
//!
//! # Examples
//!
//! ```
//! use async_std::io;
//! use futures::prelude::*;
//! use quicksink::Action;
//!
//! quicksink::make_sink(io::stdout(), |mut stdout, action| async move {
//!     match action {
//!         Action::Send(x) => stdout.write_all(x).await?,
//!         Action::Flush => stdout.flush().await?,
//!         Action::Close => stdout.close().await?
//!     }
//!     Ok::<_, io::Error>(stdout)
//! });
//! ```

use futures_core::ready;
use futures_sink::Sink;
use pin_project_lite::pin_project;
use std::{future::Future, pin::Pin, task::{Context, Poll}};

/// Returns a `Sink` impl based on the initial value and the given closure.
///
/// The closure will be applied to the initial value and an [`Action`] that
/// informs it about the action it should perform. The returned [`Future`]
/// will resolve to another value and the process starts over using this
/// output.
pub fn make_sink<S, F, T, A, E>(init: S, f: F) -> SinkImpl<S, F, T, A, E>
where
    F: FnMut(S, Action<A>) -> T,
    T: Future<Output = Result<S, E>>,
{
    SinkImpl {
        lambda: f,
        future: None,
        param: Some(init),
        state: State::Empty,
        _mark: std::marker::PhantomData
    }
}

/// The command given to the closure so that it can perform appropriate action.
///
/// Presumably the closure encapsulates a resource to perform I/O. The commands
/// correspond to methods of the [`Sink`] trait and provide the closure with
/// sufficient information to know what kind of action to perform with it.
#[derive(Debug)]
pub enum Action<A> {
    /// Send the given value.
    /// Corresponds to [`Sink::start_send`].
    Send(A),
    /// Flush the resource.
    /// Corresponds to [`Sink::poll_flush`].
    Flush,
    /// Close the resource.
    /// Corresponds to [`Sink::poll_close`].
    Close
}

/// The various states the `Sink` may be in.
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// The `Sink` is idle.
    Empty,
    /// The `Sink` is sending a value.
    Sending,
    /// The `Sink` is flushing its resource.
    Flushing,
    /// The `Sink` is closing its resource.
    Closing,
    /// The `Sink` is closed (terminal state).
    Closed
}

pin_project!
{
    /// `SinkImpl` implements the `Sink` trait.
    #[derive(Debug)]
    pub struct SinkImpl<S, F, T, A, E> {
        lambda: F,
        #[pin] future: Option<T>,
        param: Option<S>,
        state: State,
        _mark: std::marker::PhantomData<(A, E)>
    }
}

impl<S, F, T, A, E> Sink<A> for SinkImpl<S, F, T, A, E>
where
    F: FnMut(S, Action<A>) -> T,
    T: Future<Output = Result<S, E>>
{
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        match this.state {
            State::Sending | State::Flushing => {
                match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                    Ok(p) => {
                        *this.param = Some(p);
                        *this.state = State::Empty;
                        Poll::Ready(Ok(()))
                    }
                    Err(e) => Poll::Ready(Err(e))
                }
            }
            State::Closing => {
                match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                    Ok(p) => {
                        *this.param = Some(p);
                        *this.state = State::Closed;
                        Poll::Ready(Ok(()))
                    }
                    Err(e) => Poll::Ready(Err(e))
                }
            }
            State::Empty | State::Closed => Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: A) -> Result<(), Self::Error> {
        let mut this = self.project();
        assert_eq!(State::Empty, *this.state);
        if let Some(p) = this.param.take() {
            let future = (this.lambda)(p, Action::Send(item));
            this.future.set(Some(future));
            *this.state = State::Sending
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        loop {
            let mut this = self.as_mut().project();
            match this.state {
                State::Empty =>
                    if let Some(p) = this.param.take() {
                        let future = (this.lambda)(p, Action::Flush);
                        this.future.set(Some(future));
                        *this.state = State::Flushing
                    } else {
                        return Poll::Ready(Ok(()))
                    }
                State::Sending =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Flushing =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Empty;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Closing =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Closed;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Closed => return Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        loop {
            let mut this = self.as_mut().project();
            match this.state {
                State::Empty =>
                    if let Some(p) = this.param.take() {
                        let future = (this.lambda)(p, Action::Close);
                        this.future.set(Some(future));
                        *this.state = State::Closing;
                    } else {
                        return Poll::Ready(Ok(()))
                    }
                State::Sending =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Flushing =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Closing =>
                    match ready!(this.future.as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            *this.param = Some(p);
                            *this.state = State::Closed;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => return Poll::Ready(Err(e))
                    }
                State::Closed => return Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_std::{io, task};
    use futures::{prelude::*, stream};
    use crate::{Action, make_sink};

    #[test]
    fn smoke_test() {
        let sink = make_sink(io::stdout(), |mut stdout, action| async move {
            match action {
                Action::Send(x) => stdout.write_all(x).await?,
                Action::Flush => stdout.flush().await?,
                Action::Close => stdout.close().await?
            }
            Ok::<_, io::Error>(stdout)
        });

        task::block_on(async move {
            let values = vec![Ok(&b"hello\n"[..]), Ok(&b"world\n"[..])];
            assert!(stream::iter(values).forward(sink).await.is_ok())
        })
    }
}

